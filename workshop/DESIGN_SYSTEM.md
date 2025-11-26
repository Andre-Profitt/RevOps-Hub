# RevOps Command Center - Design System

## Design Philosophy

Your wireframes are functional and clear. Here's how we can elevate them while maintaining usability:

### Current Strengths (Keep These)
- Dark theme reduces eye strain for all-day use
- Clear information hierarchy
- Actionable insights, not just data
- Progress bars for quick scanning
- AI insights prominently featured

### Enhancement Opportunities
1. **Visual breathing room** - More whitespace between sections
2. **Micro-interactions** - Subtle animations for state changes
3. **Data density controls** - Let users choose detail level
4. **Contextual color** - Use color semantically, not decoratively
5. **Progressive disclosure** - Show summary, reveal detail on demand

---

## Color System

### Primary Palette

```
┌─────────────────────────────────────────────────────────────┐
│  BACKGROUNDS                                                 │
├─────────────────────────────────────────────────────────────┤
│  ████  #09090b  bg-primary      Main canvas                 │
│  ████  #18181b  bg-secondary    Cards, panels               │
│  ████  #27272a  bg-elevated     Hover states, modals        │
│  ████  #3f3f46  bg-interactive  Buttons, inputs             │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  TEXT                                                        │
├─────────────────────────────────────────────────────────────┤
│  ████  #fafafa  text-primary    Headlines, values           │
│  ████  #a1a1aa  text-secondary  Labels, descriptions        │
│  ████  #71717a  text-muted      Timestamps, metadata        │
│  ████  #52525b  text-disabled   Inactive elements           │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  SEMANTIC COLORS                                             │
├─────────────────────────────────────────────────────────────┤
│  ████  #22c55e  success         Healthy, on track, wins     │
│  ████  #eab308  warning         Monitor, caution            │
│  ████  #f97316  alert           At risk, needs attention    │
│  ████  #ef4444  critical        Critical, urgent action     │
│  ████  #3b82f6  info            Neutral information         │
│  ████  #8b5cf6  ai-accent       AI-generated content        │
└─────────────────────────────────────────────────────────────┘
```

### Health Score Gradient

```
CRITICAL     AT RISK      MONITOR      HEALTHY
   0            40           60           80         100
   ├────────────┼────────────┼────────────┼──────────┤
   #ef4444      #f97316      #eab308      #22c55e

   Usage: Background tint, not solid fill
   Example: health_bg = blend(#18181b, status_color, 0.15)
```

---

## Typography

### Font Stack

```css
/* Primary - Clean, modern, excellent number rendering */
--font-primary: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;

/* Monospace - For numbers, codes, data */
--font-mono: 'JetBrains Mono', 'SF Mono', 'Fira Code', monospace;
```

### Type Scale

```
┌────────────────────────────────────────────────────────────┐
│  SCALE                                                      │
├────────────────────────────────────────────────────────────┤
│  Display    32px / 40px    Dashboard titles                │
│  H1         24px / 32px    Section headers                 │
│  H2         18px / 24px    Card titles                     │
│  H3         14px / 20px    Subsection labels               │
│  Body       14px / 20px    General text                    │
│  Small      12px / 16px    Metadata, timestamps            │
│  Micro      10px / 14px    Badges, tags                    │
├────────────────────────────────────────────────────────────┤
│  METRICS (use monospace)                                   │
│  Hero       48px / 56px    Primary KPI value               │
│  Large      32px / 40px    Secondary metrics               │
│  Medium     24px / 32px    Table values                    │
│  Small      16px / 24px    Inline metrics                  │
└────────────────────────────────────────────────────────────┘
```

---

## Component Library

### 1. KPI Card (Enhanced)

**Before (Your wireframe):**
```
┌──────────────┐
│  ATTAINMENT  │
│     78%      │
│  ████████    │
│  vs Q3: +12% │
└──────────────┘
```

**After (Enhanced):**
```
┌────────────────────────────────────────┐
│                                        │
│  ATTAINMENT                      ↗ 12% │
│                                        │
│         78%                            │
│                                        │
│  ─────────────────────────░░░░░░░░░░  │
│  vs Q3                                 │
│                                        │
│  Target: 100%  •  Gap: 22%            │
│                                        │
└────────────────────────────────────────┘

Enhancements:
- Larger value with more whitespace
- Trend indicator in top-right (always visible)
- Secondary context in lighter text
- Progress bar with target marker
```

**CSS Spec:**
```css
.kpi-card {
  background: var(--bg-secondary);
  border: 1px solid var(--border-subtle);
  border-radius: 12px;
  padding: 20px 24px;
  min-width: 180px;
  transition: all 0.2s ease;
}

.kpi-card:hover {
  background: var(--bg-elevated);
  border-color: var(--border-hover);
  transform: translateY(-2px);
}

.kpi-card__value {
  font-family: var(--font-mono);
  font-size: 48px;
  font-weight: 600;
  letter-spacing: -0.02em;
}

.kpi-card__trend {
  font-size: 14px;
  display: flex;
  align-items: center;
  gap: 4px;
}

.kpi-card__trend--positive {
  color: var(--success);
}

.kpi-card__trend--negative {
  color: var(--critical);
}
```

---

### 2. Health Indicator (Enhanced)

**Before:**
```
Health: 55   ⚠️
```

**After:**
```
┌────────────────────────────┐
│  ●  55                     │
│  ═══════░░░░░░░░░░░░░░░   │
│  At Risk • -8 this week    │
└────────────────────────────┘

The dot color indicates status at a glance.
The bar shows position in 0-100 range.
Subtext shows category + trend.
```

**Implementation:**
```jsx
// Health indicator with semantic meaning
<HealthIndicator
  score={55}
  showBar={true}
  showTrend={true}
  trendValue={-8}
  size="medium"
/>

// Renders:
// - Colored dot (red/orange/yellow/green)
// - Score in monospace
// - Optional progress bar
// - Optional trend indicator
```

---

### 3. Comparison Bars (Enhanced)

**Before (Your wireframe):**
```
Stakeholders  You   Top 25%
Engaged       3.2     4.8
████████     ████████████
```

**After:**
```
┌─────────────────────────────────────────────────────────────┐
│  Stakeholders Engaged                                       │
│                                                             │
│  You     3.2  ████████████████████░░░░░░░░░░░░░░░░░░░░░░   │
│  Top 25% 4.8  ██████████████████████████████████░░░░░░░░   │
│                                                             │
│           Gap: 1.6 fewer  •  Impact: -15% win rate         │
└─────────────────────────────────────────────────────────────┘

Enhancements:
- Clear visual comparison with aligned bars
- Gap calculation shown
- Impact statement (why it matters)
- Hover reveals detailed breakdown
```

---

### 4. Deal Table Row (Enhanced)

**Before:**
```
MegaCorp Q4   $2.1M   Negotiation   42   38/100   STUCK, NO_EB
```

**After:**
```
┌──────────────────────────────────────────────────────────────────────────┐
│  ●  MegaCorp Q4 Renewal                                                  │
│     $2,100,000  •  Negotiation  •  42 days                              │
│                                                                          │
│     38 ━━━━━━━━░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░                   │
│                                                                          │
│     ⚠️ Stuck  ⚠️ No Economic Buyer  ⚠️ 28 days since exec meeting       │
│                                                                          │
│     [📞 Call Champion]  [📧 Email EB]  [📋 View Details]                │
└──────────────────────────────────────────────────────────────────────────┘

Enhancements:
- Status dot for instant scanning
- Risk flags as readable pills, not codes
- Inline actions (not hidden in menus)
- Health bar gives visual context
```

---

### 5. AI Insights Card (Enhanced)

**Before:**
```
🤖 AI COACHING INSIGHTS

STRENGTHS IDENTIFIED:
✓ Strong deal qualification...
```

**After:**
```
┌──────────────────────────────────────────────────────────────────────────┐
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  🤖  AI Analysis  •  Updated 2 min ago                    [↻]    │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌─ WHAT'S WORKING ─────────────────────────────────────────────────┐   │
│  │                                                                   │   │
│  │  ✓  Strong qualification                                         │   │
│  │     Your Discovery→Solution conversion is 15% above team         │   │
│  │                                                                   │   │
│  │  ✓  Fast proposal cycles                                         │   │
│  │     2.3 days faster than average to get proposals out            │   │
│  │                                                                   │   │
│  └───────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌─ FOCUS AREAS ────────────────────────────────────────────────────┐   │
│  │                                                                   │   │
│  │  ⚡  Executive engagement                                         │   │
│  │     23% of your deals have no Economic Buyer contact             │   │
│  │     Top performers: only 8%                                       │   │
│  │                                                                   │   │
│  │     → Recommended: Add EB outreach to TechVentures, CloudFirst   │   │
│  │                                                                   │   │
│  └───────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌─ THIS WEEK'S PRIORITIES ─────────────────────────────────────────┐   │
│  │                                                                   │   │
│  │  1. TechVentures ($420K)                                         │   │
│  │     Champion not identified • At risk                            │   │
│  │     [Identify Champion →]                                        │   │
│  │                                                                   │   │
│  │  2. CloudFirst ($650K)                                           │   │
│  │     Competitor involved • Need exec sponsorship                  │   │
│  │     [Request Intro →]                                            │   │
│  │                                                                   │   │
│  └───────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  [💬 Ask AI a question...]                                              │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘

Enhancements:
- Clear section headers
- Each insight has context (the "so what")
- Actionable recommendations with buttons
- Ability to ask follow-up questions
- Freshness indicator (when was this generated)
```

---

## Layout Patterns

### Dashboard Grid

```
┌──────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐           │
│  │  KPI 1  │ │  KPI 2  │ │  KPI 3  │ │  KPI 4  │ │  KPI 5  │           │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘           │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                                                                   │   │
│  │                        PRIMARY CHART                              │   │
│  │                        (Full width)                               │   │
│  │                                                                   │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌────────────────────────────┐  ┌────────────────────────────────┐     │
│  │                            │  │                                │     │
│  │      SECONDARY 1           │  │        SECONDARY 2             │     │
│  │      (Analysis)            │  │        (Analysis)              │     │
│  │                            │  │                                │     │
│  └────────────────────────────┘  └────────────────────────────────┘     │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                                                                   │   │
│  │                       ACTION TABLE                                │   │
│  │                       (Full width)                                │   │
│  │                                                                   │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌────────────────────────────┐  ┌────────────────────────────────┐     │
│  │                            │  │                                │     │
│  │     SUPPORTING 1           │  │      DETAIL PANEL              │     │
│  │                            │  │      (Context-sensitive)       │     │
│  │                            │  │                                │     │
│  └────────────────────────────┘  └────────────────────────────────┘     │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘

Spacing:
- Card gap: 16px
- Section gap: 24px
- Edge padding: 24px
- Card padding: 20px
```

---

## Micro-Interactions

### 1. Number Transitions

```css
/* When values change, animate smoothly */
.metric-value {
  transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
}

/* Count up animation for loading */
@keyframes countUp {
  from { opacity: 0; transform: translateY(10px); }
  to { opacity: 1; transform: translateY(0); }
}
```

### 2. Status Changes

```css
/* Pulse animation when status changes */
@keyframes statusPulse {
  0%, 100% { box-shadow: 0 0 0 0 var(--status-color); }
  50% { box-shadow: 0 0 0 8px transparent; }
}

.health-dot--changed {
  animation: statusPulse 1s ease-out;
}
```

### 3. Row Selection

```css
.table-row {
  transition: all 0.15s ease;
}

.table-row:hover {
  background: var(--bg-elevated);
}

.table-row--selected {
  background: var(--bg-selected);
  border-left: 3px solid var(--accent);
}
```

### 4. Loading States

```
┌────────────────────────────────────────┐
│                                        │
│  ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  │  ← Skeleton pulse
│  ░░░░░░░░░░░░░░░░░░░░                │
│                                        │
└────────────────────────────────────────┘

/* Skeleton animation */
@keyframes shimmer {
  0% { background-position: -200% 0; }
  100% { background-position: 200% 0; }
}

.skeleton {
  background: linear-gradient(
    90deg,
    var(--bg-secondary) 25%,
    var(--bg-elevated) 50%,
    var(--bg-secondary) 75%
  );
  background-size: 200% 100%;
  animation: shimmer 1.5s infinite;
}
```

---

## Responsive Breakpoints

```css
/* Desktop-first approach */
:root {
  --container-width: 1440px;
}

/* Large screens */
@media (min-width: 1440px) {
  .dashboard-grid { grid-template-columns: repeat(12, 1fr); }
}

/* Standard desktop */
@media (max-width: 1439px) {
  .dashboard-grid { grid-template-columns: repeat(12, 1fr); }
  .kpi-card { min-width: 160px; }
}

/* Tablet */
@media (max-width: 1024px) {
  .dashboard-grid { grid-template-columns: repeat(8, 1fr); }
  .secondary-panel { grid-column: span 4; }
}

/* Mobile */
@media (max-width: 768px) {
  .dashboard-grid { grid-template-columns: 1fr; }
  .kpi-row { display: grid; grid-template-columns: repeat(2, 1fr); }
}
```

---

## Accessibility

### Color Contrast
- All text meets WCAG AA (4.5:1 for normal, 3:1 for large)
- Status colors have sufficient contrast against dark backgrounds
- Never rely on color alone - use icons/text too

### Keyboard Navigation
```
Tab: Move between interactive elements
Enter/Space: Activate buttons, expand panels
Arrow keys: Navigate table rows, chart points
Escape: Close modals, deselect
```

### Screen Reader Support
```html
<div role="region" aria-label="Pipeline Health Summary">
  <h2 id="kpi-heading">Key Metrics</h2>
  <div role="list" aria-labelledby="kpi-heading">
    <div role="listitem">
      <span class="sr-only">Attainment:</span>
      <span aria-live="polite">78 percent, up 12 percent from last quarter</span>
    </div>
  </div>
</div>
```

---

## Implementation Options

### Option A: Workshop (Fastest)
- Use Workshop's built-in widgets
- Custom CSS for styling
- Some layout limitations
- Best for: Quick deployment, native Foundry integration

### Option B: Workshop + Custom Widgets
- Extend Workshop with React components
- Full design control within panels
- Best for: Enhanced UX with Foundry integration

### Option C: Standalone React App
- Full design freedom
- Connect via Foundry APIs
- Best for: Portfolio showcase, maximum polish

### Option D: Figma Prototype
- High-fidelity mockups only
- No real data
- Best for: Design presentation, stakeholder buy-in

**Recommendation**: Start with Option A (Workshop) for functional demo, create Option D (Figma) for portfolio presentation.
