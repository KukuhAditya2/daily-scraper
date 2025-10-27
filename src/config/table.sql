CREATE TABLE sources (
    id BIGSERIAL PRIMARY KEY,
    channel_id TEXT NOT NULL UNIQUE,
    platform TEXT NOT NULL CHECK (platform IN ('telegram', 'discord', 'elfa')),
    channel_name TEXT NOT NULL
);

CREATE TABLE logs_runs (
    id BIGSERIAL PRIMARY KEY,
    channel_id TEXT NOT NULL,
    platform TEXT NOT NULL,
    pulled INTEGER NOT NULL,
    kept INTEGER NOT NULL,
);

INSERT INTO "public"."sources" ("channel_id", "platform", "channel_name") VALUES
('data/event-summary?keywords=Hyperliquid%2CHYPE%2CHyperEVM%2CHIP-3%2CHyperliquidX%2Chouse%20of%20all%20finance&timeWindow=24h&searchType=or', 'elfa', 'Hyperliquid'),
( 'data/event-summary?keywords=tokenised%20stocks%2Ctokenized%20stocks%2Con-chain%20stocks%2Con%20chain%20stocks%2Cdefi%20stocks%2Cxstocks%2Ctokenised%20equities%2Ctokenized%20equities%2Cdinari%2Cstock%20issuers&timeWindow=24h&searchType=or', 'elfa', 'tokenised'),
( 'data/trending-narratives?timeFrame=day&maxNarratives=20&maxTweetsPerNarrative=20', 'elfa', 'trending-narratives'),