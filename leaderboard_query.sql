-- This is a DuckDB SQL query over `read_json_auto('results/*.json') AS results`
-- CAR-bench AgentBeats Leaderboard
-- Metrics: Pass^k (all k trials succeed), Pass@k (≥1 of k trials succeed)

SELECT
    id, -- The AgentBeats agent ID (UUID) is always required to be the first column
    submission_num AS "#",
    -- Overall score (sort key)
    COALESCE(LTRIM(PRINTF('%.2f', pass_power_3), '0'), '-') AS "Overall Pass^3",
    -- BASE
    LTRIM(PRINTF('%.2f', base_pass_power_1), '0') AS "Base Pass^1",
    COALESCE(LTRIM(PRINTF('%.2f', base_pass_power_3), '0'), '-') AS "Base Pass^3",
    COALESCE(LTRIM(PRINTF('%.2f', base_pass_at_3), '0'), '-') AS "Base Pass@3",
    -- HALLUCINATION
    LTRIM(PRINTF('%.2f', hall_pass_power_1), '0') AS "Hallucination Pass^1",
    COALESCE(LTRIM(PRINTF('%.2f', hall_pass_power_3), '0'), '-') AS "Hallucination Pass^3",
    COALESCE(LTRIM(PRINTF('%.2f', hall_pass_at_3), '0'), '-') AS "Hallucination Pass@3",
    -- DISAMBIGUATION
    LTRIM(PRINTF('%.2f', dis_pass_power_1), '0') AS "Disambiguation Pass^1",
    COALESCE(LTRIM(PRINTF('%.2f', dis_pass_power_3), '0'), '-') AS "Disambiguation Pass^3",
    COALESCE(LTRIM(PRINTF('%.2f', dis_pass_at_3), '0'), '-') AS "Disambiguation Pass@3",
    -- TIME
    CAST(ROUND(time_used, 1) AS VARCHAR) AS "Time (s)"
FROM ( -- The AgentBeats app automatically reads the JSON results into this table
    SELECT
        CAST(results.participants.agent AS VARCHAR) AS id,
        ROW_NUMBER() OVER (PARTITION BY results.participants.agent ORDER BY res.pass_power_k_scores."Pass^3" DESC) AS submission_num,
        -- Overall Pass^3 (primary sort key)
        res.pass_power_k_scores."Pass^3" AS pass_power_3,
        -- Time
        res.time_used AS time_used,
        -- Base split metrics
        res.pass_power_k_scores_by_split.base."Pass^1" AS base_pass_power_1,
        res.pass_power_k_scores_by_split.base."Pass^3" AS base_pass_power_3,
        res.pass_at_k_scores_by_split.base."Pass@3" AS base_pass_at_3,
        -- Hallucination split metrics
        res.pass_power_k_scores_by_split.hallucination."Pass^1" AS hall_pass_power_1,
        res.pass_power_k_scores_by_split.hallucination."Pass^3" AS hall_pass_power_3,
        res.pass_at_k_scores_by_split.hallucination."Pass@3" AS hall_pass_at_3,
        -- Disambiguation split metrics
        res.pass_power_k_scores_by_split.disambiguation."Pass^1" AS dis_pass_power_1,
        res.pass_power_k_scores_by_split.disambiguation."Pass^3" AS dis_pass_power_3,
        res.pass_at_k_scores_by_split.disambiguation."Pass@3" AS dis_pass_at_3
    FROM results
    CROSS JOIN UNNEST(results.results) AS r(res)
    WHERE results.participants.agent IS NOT NULL
) AS agent_metrics
ORDER BY pass_power_3 DESC;
