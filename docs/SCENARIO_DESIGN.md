# RevOps Command Center - Q4 Crunch Scenario Design

## The Story We're Telling

This isn't random data. It's a carefully crafted scenario that demonstrates
the power of AI-driven revenue operations. Every data point serves the narrative.

---

## COMPANY CONTEXT

**Company:** TechFlow Inc. (fictional B2B SaaS company)
**Product:** Enterprise workflow automation platform
**ACV Range:** $15K - $2M
**Sales Cycle:** 30-90 days (varies by segment)
**Team Size:** 10 AEs, 4 Managers, 2 Directors, 1 CRO

### Q4 Targets

| Metric | Target | Current | Gap |
|--------|--------|---------|-----|
| Revenue | $13.0M | $8.2M closed | $4.8M to go |
| New Logos | 45 | 31 | 14 to go |
| Expansion | $3.0M | $2.1M | $0.9M to go |
| Win Rate | 28% | 24% | -4pp |

---

## THE CAST OF CHARACTERS

### Sales Reps (The Heroes & The Struggling)

| Rep | Role | Story Arc | Q4 Attainment | Key Insight |
|-----|------|-----------|---------------|-------------|
| **Jessica Taylor** | Top Performer | Crushing it, potential promotion | 142% | Multi-threads every deal (4.2 avg stakeholders) |
| **Sarah Chen** | Solid Performer | Steady, reliable | 92% | Strong discovery, longer cycles |
| **David Kim** | Rising Star | Improving fast | 108% | Learned from Jessica's playbook |
| **Emily Rodriguez** | Enterprise Closer | Big deals, lumpy | 115% | 2 whale deals carrying her |
| **James Wilson** | Veteran Slump | Was great, now struggling | 78% | Deals stalling in negotiation |
| **Robert Johnson** | Steady Eddie | Always hits, never exceeds | 88% | Needs to stretch |
| **Lisa Wang** | At Risk | Multiple deals fell through | 65% | Single-threaded, going dark |
| **Amanda Foster** | New Rep | Still ramping | 72% | Good activity, poor conversion |
| **Daniel Martinez** | Turnaround | Was struggling, now improving | 95% | Coaching working |
| **Kevin Brown** | Struggling | Newest, needs help | 55% | Qualification issues |

### Managers (The Leaders)

| Manager | Region | Team | Story |
|---------|--------|------|-------|
| **Michael Torres** | West | Sarah, James, Emily | Best region, but James needs help |
| **Jennifer Adams** | East | David, Lisa, Robert | Lisa at risk of missing badly |
| **Christopher Lee** | Central | Amanda, Daniel, Jessica | Jessica carrying the team |
| **Rachel Green** | South | Kevin | New manager, struggling rep |

### Key Accounts (The Deals That Matter)

| Account | Deal | Amount | Status | Drama |
|---------|------|--------|--------|-------|
| **Acme Corporation** | Enterprise Platform | $1,200,000 | 🚨 AT RISK | Champion went dark 8 days ago, was in Commit |
| **GlobalTech Industries** | Expansion | $850,000 | ✅ HEALTHY | Multi-threaded, exec sponsor engaged |
| **Pacific Financial** | New Logo | $950,000 | ⚠️ MONITOR | Competitor involved, needs differentiation |
| **Meridian Healthcare** | Enterprise | $720,000 | ✅ CLOSING | Verbal commit, contract in legal |
| **Vertex Software** | Mid-Market | $180,000 | 🚨 STUCK | In proposal stage for 45 days |
| **Summit Education** | Expansion | $220,000 | ✅ HEALTHY | Champion strong, budget approved |
| **TechRival Customer** | Competitive | $340,000 | ❌ LOST | Lost to competitor on price |

---

## THE INSIGHTS THE SYSTEM SURFACES

### Critical Alerts (Immediate Action)

1. **"🚨 DEAL HEALTH CRASH: Acme Corporation"**
   - $1.2M deal dropped from 78 to 48 health score
   - Last activity: 8 days ago (was daily contact)
   - Risk: Champion Jennifer Chen not responding
   - Action: CRO has relationship with their CTO - needs to reach out

2. **"🚨 FORECAST AT RISK: $2.1M in Commit has health < 60"**
   - 3 deals in Commit category are showing warning signs
   - Combined risk: Could miss quarter by $1.5M
   - Action: Executive deal review needed this week

### Pattern Insights (Strategic)

3. **"📊 WINNING PATTERN: Multi-threading correlates with 2.3x win rate"**
   - Jessica's deals: 4.2 avg stakeholders → 38% win rate
   - Team average: 2.1 stakeholders → 24% win rate
   - Action: Coach team on stakeholder mapping

4. **"📉 COMPETITIVE PRESSURE: TechRival winning on price"**
   - Lost 4 deals to TechRival this quarter ($1.2M)
   - 3 of 4 cited "price" as primary reason
   - Action: Review competitive positioning, consider pricing response

5. **"⏱️ PROCESS BOTTLENECK: Legal review averaging 12 days"**
   - Benchmark: 5 days
   - 6 deals currently stuck in legal
   - Revenue at risk: $2.8M
   - Action: Escalate to legal, consider parallel processing

### Rep Coaching Insights

6. **"👤 REP ALERT: Kevin Brown needs immediate coaching"**
   - 55% attainment, lowest on team
   - Pattern: Single-threaded deals (1.3 avg stakeholders)
   - Pattern: Poor discovery (deals dying in Solution Design)
   - Action: Pair with Jessica for shadowing

7. **"⭐ REP SPOTLIGHT: Jessica Taylor ready for promotion"**
   - 142% attainment, highest on team
   - Winning patterns identified and documented
   - Action: Promote to Senior AE, formalize mentorship

---

## DATA GENERATION RULES

### Opportunity Distribution

```
Total Opportunities: 500

By Stage:
- Closed Won: 89 (18%)      ← Hit 28% win rate target
- Closed Lost: 230 (46%)    ← Realistic loss rate
- Open Pipeline: 181 (36%)  ← Active deals

By Segment:
- Enterprise (>$500K): 45 deals, $28M pipeline
- Mid-Market ($100K-$500K): 180 deals, $32M pipeline
- SMB (<$100K): 275 deals, $12M pipeline

By Health (Open Only):
- Healthy (80-100): 35%
- Monitor (60-79): 30%
- At Risk (40-59): 25%
- Critical (0-39): 10%
```

### Activity Patterns

```
Healthy Deals:
- Last activity: 0-3 days ago
- Activity frequency: 3-5 per week
- Stakeholder growth: Adding contacts over time

At Risk Deals:
- Last activity: 10-21 days ago
- Activity frequency: Dropped off
- Single point of contact

Closed Won Patterns:
- 4+ stakeholders
- Executive sponsor identified
- Consistent weekly engagement
- Champion responded within 24 hours

Closed Lost Patterns:
- 1-2 stakeholders
- No executive sponsor
- Gaps in engagement (7+ days)
- Competitor mentioned
```

### Process Mining Events

```
Happy Path (avg 45 days):
Lead Created → Qualified → Discovery → Solution Design → Proposal →
Negotiation → Legal Review → Closed Won

Common Deviations:
- Stuck in Solution Design (no technical buy-in)
- Rework loop: Proposal → Discovery → Proposal (requirements changed)
- Legal bottleneck (12 days avg vs 5 day target)
- Procurement surprise (adds 14 days)

Bottlenecks to Discover:
1. Legal Review: 12 days avg (target: 5)
2. Security Review: 8 days avg (target: 3)
3. Proposal → Negotiation: 40% fall back to Discovery
```

---

## DEMO FLOW

### Scene 1: "The Monday Morning" (2 min)
*CRO opens the Command Center on Monday morning*

- Show: Dashboard with week's critical alerts
- Highlight: Acme deal crash notification
- Action: Click through to deal detail

### Scene 2: "The Deal Crisis" (3 min)
*Drilling into the Acme situation*

- Show: Health score history (was 78, now 48)
- Show: Activity timeline (gap visible)
- Show: AI recommendation: "CRO relationship with CTO"
- Show: Similar deals that recovered

### Scene 3: "The Pattern Discovery" (2 min)
*Understanding why some reps win more*

- Show: Rep performance comparison
- Show: Jessica vs Kevin stakeholder analysis
- Show: AI insight about multi-threading
- Action: Click to see recommended coaching

### Scene 4: "The Forecast Reality" (2 min)
*Executive view of the quarter*

- Show: Forecast waterfall (Closed → Commit → Best Case)
- Show: Health-adjusted forecast (lower confidence)
- Show: Risk concentration (3 big deals at risk)
- Show: What-if: "If Acme slips, we miss by $800K"

### Scene 5: "The Process Problem" (2 min)
*Finding systemic issues*

- Show: Process mining visualization
- Show: Legal review bottleneck
- Show: Revenue stuck in each stage
- Action: Drill to specific stuck deals

### Scene 6: "The Action Plan" (1 min)
*AI-generated recommendations*

- Show: Prioritized action list
- Show: Assigned owners
- Show: Expected impact
- Close: "This is how AI transforms RevOps"

---

## FILES TO CREATE

1. `generate_scenario_opportunities.py` - Story-driven opportunity data
2. `generate_scenario_accounts.py` - Accounts that match the story
3. `generate_scenario_reps.py` - Reps with their performance patterns
4. `generate_scenario_activities.py` - Activities that show patterns
5. `generate_process_events.py` - Event log for process mining
6. `generate_forecast_snapshots.py` - Historical forecast for trending
7. `demo_queries.sql` - SQL queries for live demo
8. `aip_agents.yaml` - AIP agent configurations
9. `DEMO_SCRIPT.md` - What to say and show
