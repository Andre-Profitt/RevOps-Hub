-- ================================================================
-- REVOPS COMMAND CENTER - DEMO QUERIES
-- ================================================================
-- Run these queries in Foundry SQL or Quiver to demonstrate
-- the insights the platform provides.
--
-- Prerequisites: Build the scenario data transforms first
-- ================================================================


-- ================================================================
-- SCENE 1: EXECUTIVE DASHBOARD - "How are we doing?"
-- ================================================================

-- Q1: Q4 Forecast Summary
-- Shows: $8.2M closed, $4.8M to go, forecast breakdown
SELECT
    SUM(CASE WHEN stage_name = 'Closed Won' THEN amount ELSE 0 END) as closed_won,
    SUM(CASE WHEN forecast_category = 'Commit' AND stage_name != 'Closed Won' THEN amount ELSE 0 END) as commit_open,
    SUM(CASE WHEN forecast_category = 'Best Case' THEN amount ELSE 0 END) as best_case,
    SUM(CASE WHEN forecast_category = 'Pipeline' THEN amount ELSE 0 END) as pipeline,
    SUM(CASE WHEN stage_name = 'Closed Won' THEN amount ELSE 0 END) +
    SUM(CASE WHEN forecast_category = 'Commit' AND stage_name != 'Closed Won' THEN amount ELSE 0 END) +
    SUM(CASE WHEN forecast_category = 'Best Case' THEN amount ELSE 0 END) as total_forecast
FROM `/RevOps/Scenario/opportunities`
WHERE close_date >= '2024-10-01' AND close_date <= '2024-12-31';


-- Q2: Win Rate This Quarter
-- Shows: 24% win rate (below 28% target)
SELECT
    COUNT(CASE WHEN stage_name = 'Closed Won' THEN 1 END) as wins,
    COUNT(CASE WHEN stage_name = 'Closed Lost' THEN 1 END) as losses,
    ROUND(
        COUNT(CASE WHEN stage_name = 'Closed Won' THEN 1 END) * 100.0 /
        NULLIF(COUNT(CASE WHEN stage_name IN ('Closed Won', 'Closed Lost') THEN 1 END), 0)
    , 1) as win_rate_pct
FROM `/RevOps/Scenario/opportunities`
WHERE close_date >= '2024-10-01' AND close_date <= '2024-12-31'
  AND stage_name IN ('Closed Won', 'Closed Lost');


-- Q3: Pipeline Health Distribution
-- Shows: Distribution of deal health across open pipeline
SELECT
    CASE
        WHEN health_score_override >= 80 THEN 'Healthy'
        WHEN health_score_override >= 60 THEN 'Monitor'
        WHEN health_score_override >= 40 THEN 'At Risk'
        ELSE 'Critical'
    END as health_category,
    COUNT(*) as deal_count,
    SUM(amount) as total_amount,
    ROUND(AVG(health_score_override), 1) as avg_health
FROM `/RevOps/Scenario/opportunities`
WHERE is_closed = false
GROUP BY 1
ORDER BY avg_health DESC;


-- ================================================================
-- SCENE 2: DEAL CRISIS - "The Acme Problem"
-- ================================================================

-- Q4: Hero Deals Needing Attention
-- Shows: Named critical deals with their issues
SELECT
    opportunity_name,
    account_name,
    amount,
    stage_name,
    forecast_category,
    health_score_override as health_score,
    days_in_current_stage,
    DATEDIFF('2024-11-15', last_activity_date) as days_since_activity,
    stakeholder_count,
    has_champion,
    story_notes
FROM `/RevOps/Scenario/opportunities`
WHERE is_hero_deal = true
  AND is_closed = false
ORDER BY health_score_override ASC;


-- Q5: Deals in Commit That Are At Risk
-- Shows: $2.1M at risk that was in commit
SELECT
    account_name,
    amount,
    health_score_override as health_score,
    CASE
        WHEN health_score_override >= 80 THEN 'Healthy'
        WHEN health_score_override >= 60 THEN 'Monitor'
        WHEN health_score_override >= 40 THEN 'At Risk'
        ELSE 'Critical'
    END as health_status,
    owner_name,
    days_in_current_stage,
    story_notes
FROM `/RevOps/Scenario/opportunities`
WHERE forecast_category = 'Commit'
  AND is_closed = false
  AND health_score_override < 70
ORDER BY amount DESC;


-- ================================================================
-- SCENE 3: PATTERN DISCOVERY - "Why Jessica Wins"
-- ================================================================

-- Q6: Stakeholder Analysis by Rep (THE KEY INSIGHT)
-- Shows: Jessica's 4.2 avg vs team's 2.1
SELECT
    rep_name,
    avg_stakeholders,
    win_rate,
    ytd_attainment,
    story
FROM `/RevOps/Scenario/sales_reps`
ORDER BY win_rate DESC;


-- Q7: Win Rate vs Stakeholder Count Correlation
-- Shows: Multi-threading correlates with winning
SELECT
    CASE
        WHEN stakeholder_count >= 4 THEN '4+ stakeholders'
        WHEN stakeholder_count >= 3 THEN '3 stakeholders'
        WHEN stakeholder_count >= 2 THEN '2 stakeholders'
        ELSE '1 stakeholder'
    END as stakeholder_bucket,
    COUNT(CASE WHEN stage_name = 'Closed Won' THEN 1 END) as wins,
    COUNT(CASE WHEN stage_name = 'Closed Lost' THEN 1 END) as losses,
    ROUND(
        COUNT(CASE WHEN stage_name = 'Closed Won' THEN 1 END) * 100.0 /
        NULLIF(COUNT(*), 0)
    , 1) as win_rate_pct
FROM `/RevOps/Scenario/opportunities`
WHERE stage_name IN ('Closed Won', 'Closed Lost')
GROUP BY 1
ORDER BY win_rate_pct DESC;


-- Q8: Jessica vs Kevin Comparison (Coaching Opportunity)
SELECT
    rep_name,
    win_rate * 100 as win_rate_pct,
    avg_stakeholders,
    avg_sales_cycle_days,
    ytd_attainment * 100 as attainment_pct,
    activities_this_month,
    story
FROM `/RevOps/Scenario/sales_reps`
WHERE rep_name IN ('Jessica Taylor', 'Kevin Brown');


-- ================================================================
-- SCENE 4: COMPETITIVE PRESSURE - "TechRival Problem"
-- ================================================================

-- Q9: Losses to Competitors
-- Shows: TechRival winning 4 deals on price
SELECT
    competitor_mentioned,
    COUNT(*) as deals_lost,
    SUM(amount) as revenue_lost,
    ARRAY_JOIN(COLLECT_SET(loss_reason), ', ') as loss_reasons
FROM `/RevOps/Scenario/opportunities`
WHERE stage_name = 'Closed Lost'
  AND competitor_mentioned IS NOT NULL
GROUP BY competitor_mentioned
ORDER BY revenue_lost DESC;


-- Q10: Loss Reasons Analysis
SELECT
    loss_reason,
    COUNT(*) as count,
    SUM(amount) as total_amount,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as pct_of_losses
FROM `/RevOps/Scenario/opportunities`
WHERE stage_name = 'Closed Lost'
  AND loss_reason IS NOT NULL
GROUP BY loss_reason
ORDER BY count DESC;


-- ================================================================
-- SCENE 5: PROCESS BOTTLENECKS
-- ================================================================

-- Q11: Stage Duration Analysis (Process Mining)
-- Shows: Legal taking 12 days vs 5 day target
SELECT
    activity,
    COUNT(*) as occurrences,
    ROUND(AVG(duration_days), 1) as avg_duration,
    benchmark_days as target_duration,
    ROUND(AVG(deviation_days), 1) as avg_deviation,
    SUM(CASE WHEN is_bottleneck THEN 1 ELSE 0 END) as bottleneck_count
FROM `/RevOps/Scenario/process_events`
WHERE activity NOT IN ('Lead Created', 'Closed Won', 'Closed Lost')
GROUP BY activity, benchmark_days
ORDER BY avg_deviation DESC;


-- Q12: Deals Currently Stuck in Legal
SELECT
    o.account_name,
    o.amount,
    o.owner_name,
    e.duration_days as days_in_legal,
    e.benchmark_days as target_days,
    e.duration_days - e.benchmark_days as days_over
FROM `/RevOps/Scenario/opportunities` o
JOIN `/RevOps/Scenario/process_events` e
  ON o.opportunity_id = e.case_id
WHERE e.activity = 'Legal Review'
  AND o.is_closed = false
  AND e.duration_days > 7
ORDER BY e.duration_days DESC;


-- Q13: Rework Analysis (Deals that went backwards)
SELECT
    case_id,
    account_name,
    amount,
    activity as repeated_stage,
    resource as rep
FROM `/RevOps/Scenario/process_events`
WHERE is_rework = true
ORDER BY amount DESC;


-- ================================================================
-- SCENE 6: REP PERFORMANCE & COACHING
-- ================================================================

-- Q14: Rep Performance Leaderboard
SELECT
    rep_name,
    region,
    ROUND(ytd_attainment * 100, 0) as attainment_pct,
    ROUND(win_rate * 100, 0) as win_rate_pct,
    avg_stakeholders,
    avg_sales_cycle_days,
    current_pipeline,
    story
FROM `/RevOps/Scenario/sales_reps`
ORDER BY ytd_attainment DESC;


-- Q15: Reps Needing Coaching (Below 70% Attainment)
SELECT
    rep_name,
    manager_name,
    region,
    ROUND(ytd_attainment * 100, 0) as attainment_pct,
    ROUND(win_rate * 100, 0) as win_rate_pct,
    avg_stakeholders,
    deals_in_pipeline,
    story
FROM `/RevOps/Scenario/sales_reps`
WHERE ytd_attainment < 0.70
ORDER BY ytd_attainment ASC;


-- Q16: Manager Effectiveness (Team Roll-up)
SELECT
    manager_name,
    region,
    COUNT(*) as team_size,
    ROUND(AVG(ytd_attainment) * 100, 0) as avg_team_attainment,
    ROUND(AVG(win_rate) * 100, 0) as avg_team_win_rate,
    ROUND(SUM(current_pipeline), 0) as team_pipeline,
    SUM(CASE WHEN ytd_attainment < 0.70 THEN 1 ELSE 0 END) as reps_at_risk
FROM `/RevOps/Scenario/sales_reps`
GROUP BY manager_name, region
ORDER BY avg_team_attainment DESC;


-- ================================================================
-- BONUS: FORECAST CONFIDENCE ANALYSIS
-- ================================================================

-- Q17: Health-Weighted Forecast
-- Adjusts forecast based on deal health
SELECT
    forecast_category,
    COUNT(*) as deals,
    SUM(amount) as raw_amount,
    SUM(amount * CASE
        WHEN health_score_override >= 80 THEN 1.0
        WHEN health_score_override >= 60 THEN 0.85
        WHEN health_score_override >= 40 THEN 0.60
        ELSE 0.30
    END) as health_adjusted_amount,
    ROUND(AVG(health_score_override), 0) as avg_health
FROM `/RevOps/Scenario/opportunities`
WHERE is_closed = false
  AND close_date <= '2024-12-31'
GROUP BY forecast_category
ORDER BY
    CASE forecast_category
        WHEN 'Commit' THEN 1
        WHEN 'Best Case' THEN 2
        WHEN 'Pipeline' THEN 3
    END;


-- ================================================================
-- DATA QUALITY CHECKS
-- ================================================================

-- Verify data loaded correctly
SELECT 'Opportunities' as dataset, COUNT(*) as records FROM `/RevOps/Scenario/opportunities`
UNION ALL
SELECT 'Sales Reps' as dataset, COUNT(*) as records FROM `/RevOps/Scenario/sales_reps`
UNION ALL
SELECT 'Process Events' as dataset, COUNT(*) as records FROM `/RevOps/Scenario/process_events`;
