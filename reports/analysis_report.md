# Slack Metadata Gap Analysis Report

**Channel:** #data-questions
**Period:** 2025-11-01 to 2026-01-31
**Generated:** 2026-02-05 11:30:35

## Executive Summary

- **Threads Analyzed:** 23
- **Unique Assets Identified:** 24
- **Assets with Questions:** 24

## Priority Assets for Metadata Curation

### revenue_daily_v2

**Asset Type:** Table
**Priority Score:** 10.0/10

**Demand Signals:**
- Number of questions: 5
- Unique questioners: 5
- Question complexity: High
- Question types: Access, Unknown, Usage, Definitional

**Extracted Context:**
- *Business Context:* Adding to David's point - revenue_daily_v2 reconciles to our financial systems. Always use v2 for anything finance-related. The Finance Data team owns this table. | This gets asked weekly! finance.rev...
- *Ownership:* The Finance Data Team, Finance Data Team
- *Related Terms:* Revenue

**Sample Questions:**
> Can someone explain what the `revenue_daily` table contains vs `revenue_daily_v2`? I see both in the warehouse and not sure which one to use.
> Can someone explain what revenue_daily_v2 is? Need it for a report.
> Which revenue table should I use? I keep hearing different things.

---

### dim_customer

**Asset Type:** Table
**Priority Score:** 10.0/10

**Demand Signals:**
- Number of questions: 6
- Unique questioners: 6
- Question complexity: High
- Question types: Ownership, Definitional, Lineage, Usage

**Extracted Context:**
- *Ownership:* jennifer, The Customer Data Platform Team, jennifer.lee
- *Related Terms:* Subscription, Customer, Order

**Sample Questions:**
> Where does the data in the customer_360 dashboard come from? I need to understand the underlying tables for my analysis.
> Who owns the customers.dim_customer table? I need to request a new column.
> Where does PII flow in the data warehouse? Need this for our compliance audit.

---

### customers.dim_customer

**Asset Type:** Table
**Priority Score:** 9.5/10

**Demand Signals:**
- Number of questions: 4
- Unique questioners: 4
- Question complexity: High
- Question types: Ownership, Lineage, Usage

**Extracted Context:**
- *Ownership:* jennifer, The Customer Data Platform Team, jennifer.lee
- *Related Terms:* Customer

**Sample Questions:**
> Where does the data in the customer_360 dashboard come from? I need to understand the underlying tables for my analysis.
> Who owns the customers.dim_customer table? I need to request a new column.
> Where does PII flow in the data warehouse? Need this for our compliance audit.

---

### fct_orders

**Asset Type:** Table
**Priority Score:** 9.0/10

**Demand Signals:**
- Number of questions: 4
- Unique questioners: 4
- Question complexity: Medium
- Question types: Lineage, Definitional

**Extracted Context:**
- *Description:* Enumeration values: {1: 'pending', 2: 'confirmed', 3: 'shipped', 4: 'delivered', 5: 'cancelled', 6: 'refunded'}
- *Related Terms:* Customer

**Sample Questions:**
> Where does the data in the customer_360 dashboard come from? I need to understand the underlying tables for my analysis.
> What does the status column mean in orders.fct_orders? I see values like 1, 2, 3, 4, 5.
> Where does PII flow in the data warehouse? Need this for our compliance audit.

---

### finance.revenue_daily_v2

**Asset Type:** Table
**Priority Score:** 9.0/10

**Demand Signals:**
- Number of questions: 3
- Unique questioners: 3
- Question complexity: High
- Question types: Access, Unknown, Usage

**Extracted Context:**
- *Business Context:* This gets asked weekly! finance.revenue_daily_v2 contains daily revenue aggregations including: gross_revenue, refunds, net_revenue, by product_line and region. It's the source of truth that reconcile...
- *Ownership:* Finance Data Team
- *Related Terms:* Revenue

**Sample Questions:**
> Which revenue table should I use? I keep hearing different things.
> How do I get access to the finance.revenue_daily_v2 table?
> Revenue daily v2 - what exactly is included in this table again?

---

### revenue_daily

**Asset Type:** Table
**Priority Score:** 5.6/10

**Demand Signals:**
- Number of questions: 2
- Unique questioners: 2
- Question complexity: Medium
- Question types: Usage, Definitional

**Extracted Context:**
- *Business Context:* Adding to David's point - revenue_daily_v2 reconciles to our financial systems. Always use v2 for anything finance-related. The Finance Data team owns this table. | ALWAYS use finance.revenue_daily_v2...
- *Ownership:* The Finance Data Team
- *Related Terms:* Revenue

**Sample Questions:**
> Can someone explain what the `revenue_daily` table contains vs `revenue_daily_v2`? I see both in the warehouse and not sure which one to use.
> Which revenue table should I use? I keep hearing different things.

---

### orders.fct_orders

**Asset Type:** Table
**Priority Score:** 5.6/10

**Demand Signals:**
- Number of questions: 2
- Unique questioners: 2
- Question complexity: Medium
- Question types: Lineage, Definitional

**Extracted Context:**
- *Description:* Enumeration values: {1: 'pending', 2: 'confirmed', 3: 'shipped', 4: 'delivered', 5: 'cancelled', 6: 'refunded'}

**Sample Questions:**
> What does the status column mean in orders.fct_orders? I see values like 1, 2, 3, 4, 5.
> Where does PII flow in the data warehouse? Need this for our compliance audit.

---

### analytics.events

**Asset Type:** Table
**Priority Score:** 5.1/10

**Demand Signals:**
- Number of questions: 2
- Unique questioners: 2
- Question complexity: Low
- Question types: Definitional

**Extracted Context:**
- *Description:* Enumeration values: {1: 'signup', 2: 'trial_start', 3: 'paid_conversion', 4: 'upgrade', 5: 'downgrade', 6: 'churn'}
- *Gotchas/Caveats:* 1 noted
- *Related Terms:* Churn, USER

**Sample Questions:**
> What does conversion_type = 3 mean in the analytics.events table? I can't find any documentation.
> What's the grain of the analytics.events table? Is it one row per event or aggregated somehow?

---

### marketing.fct_campaigns

**Asset Type:** Table
**Priority Score:** 2.8/10

**Demand Signals:**
- Number of questions: 1
- Unique questioners: 1
- Question complexity: Low
- Question types: Lineage

**Extracted Context:**
- *Related Terms:* Customer

**Sample Questions:**
> Where does the data in the customer_360 dashboard come from? I need to understand the underlying tables for my analysis.

---

### sales.fct_orders

**Asset Type:** Table
**Priority Score:** 2.8/10

**Demand Signals:**
- Number of questions: 1
- Unique questioners: 1
- Question complexity: Low
- Question types: Lineage

**Extracted Context:**
- *Related Terms:* Customer

**Sample Questions:**
> Where does the data in the customer_360 dashboard come from? I need to understand the underlying tables for my analysis.

---

### fct_campaigns

**Asset Type:** Table
**Priority Score:** 2.8/10

**Demand Signals:**
- Number of questions: 1
- Unique questioners: 1
- Question complexity: Low
- Question types: Lineage

**Extracted Context:**
- *Related Terms:* Customer

**Sample Questions:**
> Where does the data in the customer_360 dashboard come from? I need to understand the underlying tables for my analysis.

---

### marketing.campaign_attribution

**Asset Type:** Table
**Priority Score:** 2.8/10

**Demand Signals:**
- Number of questions: 1
- Unique questioners: 1
- Question complexity: Low
- Question types: Quality

**Extracted Context:**
- *Ownership:* s Team
- *Gotchas/Caveats:* 1 noted

**Sample Questions:**
> Is the marketing.campaign_attribution table reliable? I'm seeing some weird numbers.

---

### product.user_metrics

**Asset Type:** Table
**Priority Score:** 2.8/10

**Demand Signals:**
- Number of questions: 1
- Unique questioners: 1
- Question complexity: Low
- Question types: Definitional

**Extracted Context:**
- *Business Context:* For investor reporting, we always use dau. That's the official KPI definition approved by leadership....
- *Related Terms:* DAU

**Sample Questions:**
> What's the difference between active_users and dau in the product.user_metrics table?

---

### jennifer.lee

**Asset Type:** Table
**Priority Score:** 2.8/10

**Demand Signals:**
- Number of questions: 1
- Unique questioners: 1
- Question complexity: Low
- Question types: Ownership

**Extracted Context:**
- *Ownership:* jennifer, The Customer Data Platform Team, jennifer.lee
- *Related Terms:* Customer

**Sample Questions:**
> Who owns the customers.dim_customer table? I need to request a new column.

---

### pii.customer_raw

**Asset Type:** Table
**Priority Score:** 2.8/10

**Demand Signals:**
- Number of questions: 1
- Unique questioners: 1
- Question complexity: Low
- Question types: Lineage

**Sample Questions:**
> Where does PII flow in the data warehouse? Need this for our compliance audit.

---

## Pattern Analysis

### Question Type Distribution

- **Lineage:** 34.1%
- **Definitional:** 29.5%
- **Usage:** 15.9%
- **Ownership:** 6.8%
- **Quality:** 4.5%
- **Access:** 4.5%
- **Unknown:** 4.5%

### Identified Metadata Gaps

#### Missing Descriptions (Severity: High)
Multiple assets lack clear descriptions, causing repeated definitional questions
- Affected assets: revenue_daily_v2, dim_customer, fct_orders, revenue_daily, orders.fct_orders

#### Missing Ownership Information (Severity: Medium)
Users frequently ask who owns certain assets
- Affected assets: dim_customer, customers.dim_customer, jennifer.lee

#### Undocumented Enumeration Values (Severity: High)
Column values/codes lack documentation causing confusion
- Affected assets: fct_orders, analytics.events, orders.fct_orders

#### Versioning/Deprecation Confusion (Severity: Medium)
Multiple versions or legacy assets exist without clear guidance
- Affected assets: revenue_daily_v2, finance.revenue_daily_v2, legacy.customers

## Workflow Agent Opportunities

### Description Agent Candidates
Assets with clear definitions extracted from Slack answers:

- **fct_orders**: "Enumeration values: {1: 'pending', 2: 'confirmed', 3: 'shipped', 4: 'delivered', 5: 'cancelled', 6: ..."
- **orders.fct_orders**: "Enumeration values: {1: 'pending', 2: 'confirmed', 3: 'shipped', 4: 'delivered', 5: 'cancelled', 6: ..."
- **analytics.events**: "Enumeration values: {1: 'signup', 2: 'trial_start', 3: 'paid_conversion', 4: 'upgrade', 5: 'downgrad..."
- **0.82**: "Enumeration values: {30: 'low risk', 60: 'medium risk', 100: 'high risk'}..."
- **ml.customer_predictions**: "Enumeration values: {30: 'low risk', 60: 'medium risk', 100: 'high risk'}..."

### Ownership Agent Candidates
Assets with identified owners in threads:

- **revenue_daily_v2**: The Finance Data Team, Finance Data Team (High confidence)
- **dim_customer**: jennifer, The Customer Data Platform Team, jennifer.lee (High confidence)
- **customers.dim_customer**: jennifer, The Customer Data Platform Team, jennifer.lee (High confidence)
- **finance.revenue_daily_v2**: Finance Data Team (Medium confidence)
- **revenue_daily**: The Finance Data Team (Medium confidence)

### Quality Context Agent Candidates
Assets with quality caveats mentioned:

- **analytics.events**: 1 quality notes (Medium severity)
- **marketing.campaign_attribution**: 1 quality notes (Medium severity)
- **product.feature_usage**: 2 quality notes (High severity)

### Glossary Linkage Agent Candidates
Assets with business terms that should be linked:

- **dim_customer**: Subscription, Customer, Order
- **finance.subscriptions**: Subscription, MRR, Revenue
- **customers.dim_customer.is_active**: Subscription, Customer, Order
- **analytics.events**: Churn, USER
- **product.feature_usage**: USER, Product
