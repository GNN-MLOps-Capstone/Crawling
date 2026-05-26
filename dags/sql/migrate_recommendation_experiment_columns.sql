-- Add experiment assignment fields to recommendation serve logs for A/B evaluation.
ALTER TABLE public.recommendation_serves
    ADD COLUMN IF NOT EXISTS experiment_id varchar(128),
    ADD COLUMN IF NOT EXISTS variant varchar(32);

CREATE OR REPLACE FUNCTION public.set_recommendation_serve_experiment_assignment()
RETURNS trigger AS $$
BEGIN
    NEW.experiment_id := COALESCE(NULLIF(NEW.experiment_id, ''), 'control');

    IF NULLIF(NEW.variant, '') IS NULL THEN
        NEW.variant := CASE
            WHEN NEW.source LIKE 'ab_latest_%' THEN 'latest'
            WHEN NEW.source LIKE 'ab_popular_%' THEN 'popular'
            WHEN NEW.source LIKE 'ab_random_%' THEN 'random'
            ELSE 'recommend'
        END;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_recommendation_serves_experiment_assignment
    ON public.recommendation_serves;

CREATE TRIGGER tr_recommendation_serves_experiment_assignment
BEFORE INSERT OR UPDATE OF experiment_id, variant, source
ON public.recommendation_serves
FOR EACH ROW
EXECUTE FUNCTION public.set_recommendation_serve_experiment_assignment();

UPDATE public.recommendation_serves
SET
    experiment_id = COALESCE(NULLIF(experiment_id, ''), 'control'),
    variant = COALESCE(
        NULLIF(variant, ''),
        CASE
            WHEN source LIKE 'ab_latest_%' THEN 'latest'
            WHEN source LIKE 'ab_popular_%' THEN 'popular'
            WHEN source LIKE 'ab_random_%' THEN 'random'
            ELSE 'recommend'
        END
    )
WHERE experiment_id IS NULL
   OR experiment_id = ''
   OR variant IS NULL
   OR variant = '';

CREATE INDEX IF NOT EXISTS ix_recommendation_serves_experiment_id
    ON public.recommendation_serves (experiment_id);

CREATE INDEX IF NOT EXISTS ix_recommendation_serves_variant
    ON public.recommendation_serves (variant);

CREATE INDEX IF NOT EXISTS ix_recommendation_serves_experiment_variant_created_at
    ON public.recommendation_serves (experiment_id, variant, created_at);
