# Customer Support Workforce Analytics & Hiring Optimisation Framework

# Project Overview
Built a data-driven workforce analytics framework to assess support demand, employee workload, and operational pressure points across 161 DSLG Bank branches in 10 countries.

The project enables leadership to make quantitative hiring decisions based on actual workload and service demand.

# Business Problem
Customer support teams reported increased workload and overtime, but leadership lacked data to determine:

- Whether additional staff were required
- Where demand was concentrated
- Which employees were overburdened

# 🗂 Data Overview
- 3,310 customers, 4,000 support queries, 16 representatives
- Coverage across 161 branches / 10 countries
- Key data points: customer interactions, query types, call duration, employee workload

# Tools & Technologies
- MySQL
- SQL (aggregations, joins, subqueries, CASE logic, custom functions)

# SQL Highlights
1. Representative Workload Analysis

select
	 SUPPORT_REPRESENTATIVE_ID,
     EMPLOYEE_NAME as REPRESENTANTIVE_NAME,
     count(SUPPORT_QUERY_ID) as NO_OF_QUERIES
from banking_support_query_employee_v
group by 1,2
order by 3 desc;

2. Average Queries per Representative

select
    count(*) / count(distinct SUPPORT_REPRESENTATIVE_ID) as AVG_QUERIES_PER_REP
from bank_support_query_t;

3. High Workload Representatives (>250 Queries)

select
	 SUPPORT_REPRESENTATIVE_ID as EMPLOYEE_ID,
     EMPLOYEE_NAME,
     count(SUPPORT_QUERY_ID) as NO_OF_QUERIES
from banking_support_query_employee_v
group by 1,2
having NO_OF_QUERIES > 250
order by 3 desc;

# Key Insights
- Support demand is high relative to workforce size:
  - 4,000 queries handled by only 16 representatives
  - Average of 250 queries per representative
- Workload imbalance exists:
  - 9 out of 16 representatives are overloaded (>250 queries)
  - Top performer handled 275 queries, indicating uneven distribution
- Customer support demand is moderate per user:
  - Average of 1.2 queries per customer
  - However, 76 customers generate high demand (≥3 queries)
- Geographic concentration of demand:
  - Jersey City (New York) has the highest query volume
  - Top states (New York, Chicago, Los Angeles) dominate customer base
- Query types are evenly distributed:
  - 5 major categories (Loans, Credit Card, etc.) with ~800 queries each
  - Indicates broad-based demand rather than a single issue driver
- Specialized demand hotspots exist:
  - Certain branches show spikes in specific queries (e.g., Home Loans)
 
# Recommendations
- Hire additional support staff (high priority)
  - Current workload exceeds optimal capacity
  - Over 50% of representatives are overloaded
- Redistribute workload more evenly
  - Balance query allocation across representatives
- Implement query routing and specialization
  - Assign reps based on query type expertise
- Focus on high-demand regions
  - Increase staffing or support resources in top cities/states
- Introduce self-service or automation
  - Reduce repetitive queries from high-frequency customers
 
# Outcome

Developed a quantitative hiring framework that clearly demonstrates workforce strain, identifies demand concentration, and supports data-backed staffing decisions.

# Next Steps
- Build real-time support dashboards
- Introduce SLA (Service Level Agreement) tracking
- Analyze call resolution time and efficiency
- Implement predictive staffing models
