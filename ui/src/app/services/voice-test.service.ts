import { Injectable, OnDestroy } from "@angular/core";
import { filter, Observable, Subscription } from "rxjs";

import {
    TauriService,
    TestSttResultMessage,
    TestSttStartedMessage,
    TestTtsResultMessage,
} from "./tauri.service";

type PendingRequest<T> = {
    resolve: (data: T) => void;
    reject: (error: Error) => void;
    timeoutId: number;
};

const STT_RESULT_TIMEOUT_BUFFER_MS = 10000;
const TTS_RESULT_TIMEOUT_MS = 20000;

@Injectable({
    providedIn: "root",
})
export class VoiceTestService implements OnDestroy {
    private sttResultSubscription: Subscription;
    private ttsResultSubscription: Subscription;
    private pendingSttRequests: PendingRequest<TestSttResultMessage>[] = [];
    private pendingTtsRequests: PendingRequest<TestTtsResultMessage>[] = [];

    public readonly sttTestStarted$: Observable<TestSttStartedMessage>;

    constructor(private tauriService: TauriService) {
        this.sttTestStarted$ = this.tauriService.output$.pipe(
            filter(
                (message): message is TestSttStartedMessage =>
                    message.type === "test_stt_started",
            ),
        );

        this.sttResultSubscription = this.tauriService.output$
            .pipe(
                filter(
                    (message): message is TestSttResultMessage =>
                        message.type === "test_stt_result",
                ),
            )
            .subscribe((message) => {
                const pending = this.pendingSttRequests.shift();
                if (!pending) {
                    return;
                }
                window.clearTimeout(pending.timeoutId);
                pending.resolve(message);
            });

        this.ttsResultSubscription = this.tauriService.output$
            .pipe(
                filter(
                    (message): message is TestTtsResultMessage =>
                        message.type === "test_tts_result",
                ),
            )
            .subscribe((message) => {
                const pending = this.pendingTtsRequests.shift();
                if (!pending) {
                    return;
                }
                window.clearTimeout(pending.timeoutId);
                pending.resolve(message);
            });
    }

    ngOnDestroy(): void {
        this.sttResultSubscription.unsubscribe();
        this.ttsResultSubscription.unsubscribe();
        for (const pending of this.pendingSttRequests) {
            window.clearTimeout(pending.timeoutId);
            pending.reject(new Error("Voice test service destroyed"));
        }
        for (const pending of this.pendingTtsRequests) {
            window.clearTimeout(pending.timeoutId);
            pending.reject(new Error("Voice test service destroyed"));
        }
        this.pendingSttRequests = [];
        this.pendingTtsRequests = [];
    }

    public testStt(durationSeconds = 5): Promise<TestSttResultMessage> {
        return new Promise(async (resolve, reject) => {
            const pending: PendingRequest<TestSttResultMessage> = {
                resolve,
                reject,
                timeoutId: window.setTimeout(() => {
                    const index = this.pendingSttRequests.indexOf(pending);
                    if (index >= 0) {
                        this.pendingSttRequests.splice(index, 1);
                    }
                    reject(new Error("Timed out waiting for STT test result"));
                }, durationSeconds * 1000 + STT_RESULT_TIMEOUT_BUFFER_MS),
            };
            this.pendingSttRequests.push(pending);
            try {
                await this.tauriService.send_command({
                    type: "test_stt",
                    duration_seconds: durationSeconds,
                    timestamp: new Date().toISOString(),
                });
            } catch (error) {
                window.clearTimeout(pending.timeoutId);
                const index = this.pendingSttRequests.indexOf(pending);
                if (index >= 0) {
                    this.pendingSttRequests.splice(index, 1);
                }
                reject(error instanceof Error ? error : new Error(String(error)));
            }
        });
    }

    public testTts(text: string): Promise<TestTtsResultMessage> {
        return new Promise(async (resolve, reject) => {
            const pending: PendingRequest<TestTtsResultMessage> = {
                resolve,
                reject,
                timeoutId: window.setTimeout(() => {
                    const index = this.pendingTtsRequests.indexOf(pending);
                    if (index >= 0) {
                        this.pendingTtsRequests.splice(index, 1);
                    }
                    reject(new Error("Timed out waiting for TTS test result"));
                }, TTS_RESULT_TIMEOUT_MS),
            };
            this.pendingTtsRequests.push(pending);
            try {
                await this.tauriService.send_command({
                    type: "test_tts",
                    text,
                    timestamp: new Date().toISOString(),
                });
            } catch (error) {
                window.clearTimeout(pending.timeoutId);
                const index = this.pendingTtsRequests.indexOf(pending);
                if (index >= 0) {
                    this.pendingTtsRequests.splice(index, 1);
                }
                reject(error instanceof Error ? error : new Error(String(error)));
            }
        });
    }
}
