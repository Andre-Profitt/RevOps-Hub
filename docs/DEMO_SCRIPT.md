# RevOps Command Center - Complete Demo Script

## Overview

**Duration**: 25-30 minutes
**Audience**: Sales leadership, RevOps, or technical evaluators
**Goal**: Show how Foundry transforms raw CRM data into actionable intelligence

**The Story**: It's November 15th, Q4 crunch time. TechFlow Solutions needs $4.8M to close this quarter. You're about to show them exactly where the risk is—and what to do about it.

---

## Pre-Demo Setup Checklist

- [ ] Foundry environment loaded with scenario data
- [ ] Quiver/SQL workbench open
- [ ] Workshop dashboard loaded (if built)
- [ ] AIP Agent Studio ready (optional live demo)
- [ ] Demo queries bookmarked or in clipboard

---

## SCENE 1: The Executive View (5 minutes)

### Setup the Context

**SAY THIS:**
> "Let's start where every Monday begins—the leadership meeting. Your CRO asks: 'How are we tracking to close the quarter?' Here's what most teams do: they open Salesforce, pull a report, and spend 20 minutes squinting at pipeline stages. Let me show you what's possible instead."

### Run Query: Q4 Forecast Summary

```sql
SELECT
    SUM(CASE WHEN stage_name = 'Closed Won' THEN amount ELSE 0 END) as closed_won,
    SUM(CASE WHEN forecast_category = 'Commit' AND stage_name != 'Closed Won' THEN amount ELSE 0 END) as commit_open,
    SUM(CASE WHEN forecast_category = 'Best Case' THEN amount ELSE 0 END) as best_case,
    SUM(CASE WHEN forecast_category = 'Pipeline' THEN amount ELSE 0 END) as pipeline
FROM `/RevOps/Scenario/opportunities`
WHERE close_date >= '2024-10-01' AND close_date <= '2024-12-31';
```

**POINT OUT:**
- $8.2M closed, $4.8M to go
- Commit: $4.2M | Best Case: $2.1M | Pipeline: $1.8M
- "Math says we make it. But let's look deeper."

### Run Query: Win Rate

```sql
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
```

**SAY THIS:**
> "24% win rate. Our target is 28%. That 4-point gap? That's $400K we're leaving on the table. But here's the thing—win rate is a trailing indicator. Let me show you where deals are actually at risk *right now*."

### Transition

> "The executive view tells us *what*. Now let's find the *why*."

---

## SCENE 2: The Deal Crisis (7 minutes)

### The Problem Reveal

**SAY THIS:**
> "Every sales org has 'hero deals'—the big ones everyone's counting on. Let me show you something concerning about your biggest Q4 deal."

### Run Query: Hero Deals Needing Attention

```sql
SELECT
    opportunity_name,
    account_name,
    amount,
    stage_name,
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
```

**THE BIG REVEAL:**
> "Acme Corporation. $1.2 million. Your second-biggest Q4 deal. Health score: 48. That's CRITICAL. Look at these numbers:
> - 18 days in Negotiation (benchmark is 14)
> - 8 days since last activity—they've gone dark
> - Only 2 stakeholders engaged—that's single-threaded
> - Champion Jennifer Chen stopped responding
>
> Two weeks ago this deal was at 78 health. Something happened."

### Pause for Impact

Let this sink in. Then:

> "The traditional approach? The rep tells their manager 'it's fine, they're just busy.' The manager tells the VP 'it's in Commit.' The VP tells the CRO 'we're on track.'
>
> Meanwhile, the deal is dying.
>
> This system doesn't rely on self-reporting. It calculates health from *behavior*—activity patterns, engagement velocity, stakeholder count. The data doesn't lie."

### Show the Commit-at-Risk View

```sql
SELECT
    account_name,
    amount,
    health_score_override as health_score,
    owner_name,
    days_in_current_stage,
    story_notes
FROM `/RevOps/Scenario/opportunities`
WHERE forecast_category = 'Commit'
  AND is_closed = false
  AND health_score_override < 70
ORDER BY amount DESC;
```

**POINT OUT:**
> "$2.1 million in 'Commit' has health scores below 70. That's not Commit—that's Best Case at best. Your forecast is overstated."

### The Action

> "So what do we do? The system doesn't just identify problems—it suggests actions. For Acme:
> 1. CRO needs to reach out to their CTO (existing board connection)
> 2. Loop in the executive sponsor we met at the conference
> 3. Get this done by end of day Tuesday—every day increases the risk"

---

## SCENE 3: The Pattern Discovery (6 minutes)

### Setup the Insight

**SAY THIS:**
> "Here's where it gets interesting. I want to show you something we discovered in the data that your team didn't know existed."

### Run Query: Stakeholder Analysis by Rep

```sql
SELECT
    rep_name,
    avg_stakeholders,
    win_rate,
    ytd_attainment,
    story
FROM `/RevOps/Scenario/sales_reps`
ORDER BY win_rate DESC;
```

**THE KEY INSIGHT:**
> "Look at Jessica Taylor. 38% win rate—highest on the team. 142% attainment. Now look at her average stakeholders: 4.2.
>
> Now look at Kevin Brown. 15% win rate. 55% attainment. His stakeholder count? 1.3.
>
> Jessica wins because she multi-threads every deal. Kevin loses because he bets on a single champion."

### Prove it with Data

```sql
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
```

**POINT OUT:**
> "This is across ALL deals, not just individual reps:
> - 4+ stakeholders: 45% win rate
> - 1 stakeholder: 12% win rate
>
> Multi-threading isn't a nice-to-have—it's a 3.7x improvement in win rate. This is a *process* insight, not a *person* insight."

### The Coaching Moment

**SAY THIS:**
> "So instead of telling Kevin 'try harder,' we tell him something specific: 'Your deals are dying because you have 1.3 stakeholders. Jessica averages 4.2. Here's exactly what she does differently.'
>
> That's coachable. That's actionable. That's how you turn a struggling rep into a performer."

---

## SCENE 4: Competitive Intelligence (4 minutes)

### Setup

**SAY THIS:**
> "Let's look at something your team probably knows intuitively but can't prove: where you're losing and why."

### Run Query: Losses to Competitors

```sql
SELECT
    competitor_mentioned,
    COUNT(*) as deals_lost,
    SUM(amount) as revenue_lost
FROM `/RevOps/Scenario/opportunities`
WHERE stage_name = 'Closed Lost'
  AND competitor_mentioned IS NOT NULL
GROUP BY competitor_mentioned
ORDER BY revenue_lost DESC;
```

**POINT OUT:**
> "TechRival. Four deals. $1.2 million lost. That's not random—that's a pattern."

### Run Query: Loss Reasons

```sql
SELECT
    loss_reason,
    COUNT(*) as count,
    SUM(amount) as total_amount
FROM `/RevOps/Scenario/opportunities`
WHERE stage_name = 'Closed Lost'
  AND loss_reason IS NOT NULL
GROUP BY loss_reason
ORDER BY count DESC;
```

**THE INSIGHT:**
> "Three of those four TechRival losses cite 'price' as the reason. But look—those are all Mid-Market deals.
>
> TechRival isn't beating you on product. They're running an aggressive pricing play in one segment. Your Enterprise deals? You're holding. Your Mid-Market? They're eating your lunch.
>
> Action: Create a Mid-Market competitive response. Maybe promotional pricing, maybe better ROI positioning, but do it specifically for that segment."

---

## SCENE 5: Process Bottlenecks (4 minutes)

### Setup

**SAY THIS:**
> "Now let me show you something your sales team complains about but can't quantify: where the process breaks down."

### Run Query: Stage Duration Analysis

```sql
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
```

**THE BOTTLENECK:**
> "Legal Review. Target: 5 days. Actual: 12 days. That's 7 extra days on EVERY enterprise deal.
>
> Your deals aren't slow because your reps are slow. They're slow because Legal is understaffed."

### Show Deals Stuck in Legal

```sql
SELECT
    o.account_name,
    o.amount,
    o.owner_name,
    e.duration_days as days_in_legal,
    e.benchmark_days as target_days
FROM `/RevOps/Scenario/opportunities` o
JOIN `/RevOps/Scenario/process_events` e
  ON o.opportunity_id = e.case_id
WHERE e.activity = 'Legal Review'
  AND o.is_closed = false
  AND e.duration_days > 7
ORDER BY e.duration_days DESC;
```

**SAY THIS:**
> "Six deals, $2.8 million, stuck waiting for legal. That's not a sales problem—that's an operations problem. The fix isn't 'better salespeople.' The fix is:
> 1. Add legal headcount or contract resource
> 2. Create standard terms templates (covers 80% of cases)
> 3. Implement an SLA with escalation path
>
> This is process mining telling you exactly where to invest."

---

## SCENE 6: The AIP Agent Demo (4 minutes)

### If AIP is Available

**SAY THIS:**
> "Everything I just showed you took querying skills. But your CRO doesn't want to write SQL. They want to ask a question and get an answer. That's what AIP does."

### Open AIP Agent

If you've configured the Deal Risk Analyzer agent:

**TYPE:**
> "Which deals are at risk this quarter?"

**EXPECTED RESPONSE:**
The agent should return something like:
> "I've identified 3 critical deals requiring immediate attention:
>
> 1. **Acme Corporation** ($1.2M) - Health: 48 (CRITICAL)
>    - Champion Jennifer Chen has gone dark (8 days no contact)
>    - Stuck in Negotiation for 18 days
>    - Only 2 stakeholders engaged
>    - **Action**: CRO executive outreach to their CTO by Tuesday
>
> 2. **Nexus Industries** ($650K) - Health: 52 (AT RISK)
>    - TechRival is actively competing
>    - Deal has gone backwards to Discovery
>    - **Action**: Request competitive analysis from SE team..."

**SAY THIS:**
> "That's the same insight I showed you in the queries—but delivered in 3 seconds to anyone on the team, in natural language.
>
> The CRO asks 'what deals are at risk?' The system tells them, with specific actions. No SQL. No dashboard drilling. Just answers."

### If AIP is Not Available

**SAY THIS:**
> "In production, all of this would be accessible through natural language. Your CRO could simply ask 'What deals are at risk?' and get exactly what I showed you—the specific deals, why they're at risk, and what to do about it."

---

## Closing: The Business Value (2 minutes)

**SAY THIS:**
> "Let me summarize what you just saw:
>
> 1. **Forecast Accuracy**: We identified $2.1M in 'Commit' that isn't really Commit. Your forecast just got more accurate.
>
> 2. **Deal Rescue**: We found Acme—a $1.2M deal that was about to slip—with enough time to save it.
>
> 3. **Coaching Insight**: We discovered that multi-threading is a 3.7x improvement in win rate. That's a training program right there.
>
> 4. **Competitive Response**: We identified TechRival's pricing strategy in Mid-Market. Now you can respond specifically.
>
> 5. **Process Improvement**: We found Legal is adding 7 days to every deal. That's a capacity investment case.
>
> This isn't a dashboard you look at. This is a system that *pushes* insights to you before you know to ask. That's the difference between reactive and proactive revenue operations."

---

## Handling Questions

### "Is this real data?"

> "This is scenario data designed to demonstrate the system's capabilities. The transforms, health scoring, and analytics are exactly what you'd deploy on your Salesforce data. The implementation takes about 2-3 weeks."

### "How does the health score work?"

> "It's a weighted composite of 5 factors: velocity (are they progressing?), activity (when was last touch?), stakeholders (multi-threaded?), competition (who else is involved?), and discount (desperation indicator). Each factor can be tuned to your specific sales motion."

### "What about data freshness?"

> "Foundry schedules would sync from Salesforce daily or more frequently. Health scores recalculate on every sync. The AIP agents always query current data."

### "Can reps see this?"

> "Yes—through Workshop apps or AIP. You'd have role-based views: reps see their own deals, managers see their team, executives see everything. Everyone gets relevant insights at their level."

### "How long to implement?"

> "With this foundation already built:
> - Week 1: Connect your Salesforce, validate schemas
> - Week 2: Tune health scoring to your benchmarks
> - Week 3: Build Workshop dashboards, train team
> - Ongoing: Iterate on insights based on feedback"

---

## Demo Variants

### Short Demo (10 minutes)
- Scene 1: Executive View (2 min)
- Scene 2: Acme deal crisis only (3 min)
- Scene 3: Jessica/Kevin stakeholder insight (3 min)
- Closing (2 min)

### Technical Deep-Dive (45 minutes)
Add:
- Walk through transform code
- Show ontology design
- Demonstrate Workshop app building
- Show schedule configuration
- Discuss error handling and data quality

### AIP-Focused Demo (15 minutes)
- Brief context (2 min)
- Live AIP conversation covering all 5 agents (10 min)
- Closing on capabilities (3 min)

---

## Appendix: Query Quick Reference

| Scene | Query | Shows |
|-------|-------|-------|
| 1 | Q1 - Forecast Summary | $8.2M closed, $4.8M to go |
| 1 | Q2 - Win Rate | 24% vs 28% target |
| 2 | Q4 - Hero Deals | Acme at health 48 |
| 2 | Q5 - Commit at Risk | $2.1M questionable |
| 3 | Q6 - Stakeholder by Rep | Jessica vs Kevin |
| 3 | Q7 - Win Rate Correlation | 4+ stakeholders = 45% |
| 4 | Q9 - Competitor Losses | TechRival taking $1.2M |
| 4 | Q10 - Loss Reasons | Price vs Product |
| 5 | Q11 - Stage Duration | Legal = 12 days |
| 5 | Q12 - Stuck in Legal | $2.8M waiting |
