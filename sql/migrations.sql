CREATE TABLE IF NOT EXISTS narrador_jobs (
  id           UUID         PRIMARY KEY,
  created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
  prompt       TEXT         NOT NULL,
  model        TEXT         NOT NULL,
  voice        TEXT         NOT NULL,
  speed        NUMERIC      NOT NULL DEFAULT 1.0,
  web_search   BOOLEAN      NOT NULL DEFAULT false,
  status       TEXT         NOT NULL DEFAULT 'pending',
  -- pending → writing → recording → done | error
  progress_text TEXT,
  progress_pct  INT          DEFAULT 0,
  text_chars    INT,
  search_count  INT          DEFAULT 0,
  audio_key     TEXT,        -- S3 key: narrador/<uuid>.mp3
  audio_size    INT,
  error         TEXT,
  title         TEXT
);

CREATE INDEX IF NOT EXISTS narrador_jobs_created_idx ON narrador_jobs(created_at DESC);
