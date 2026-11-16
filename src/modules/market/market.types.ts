export type SessionMode = 
    | "SCHEDULED"
    | "MANUAL";

export type SessionStatus =
    | "RUNNING"
    | "STOPPED"
    | "SKIPPED"
    | "FAILED";

export type StopReason =
    | "DURATION_COMPLETED"
    | "MANUAL_STOP"
    | "MANUAL_CONFLICT"
    | "APPLICATION_RESTART"
    | "COLLECTOR_ERROR";

export interface CollectionSession {
    id: string;
    mode: SessionMode;
    status: SessionStatus;
    startedAt: Date;
    scheduledStopAt: Date;
    scheduledFor: Date | null;
    stoppedAt: Date | null;
    stopReason: string | null;
}
