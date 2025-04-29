# Introduction 
This repository supports Sembrando Sentido's mission to promote accessible transparency regarding government disaster relief aid in Puerto Rico, specifically focused on the R3 Program (Repair, Reconstruction, and Relocation). It includes tools for building an interactive dashboard and managing a data pipeline that extracts, processes, and streams data into a centralized PostgreSQL database. The goal is to track and assess the flow of CDBG funds across various stages of the recovery process.

## Streaming

The ```streaming``` directory contains scripts for automating the ingestion and updating of PostgreSQL tables. These scripts regularly scrape data and sync with remote sources, primarily focusing on Stage 4 and Stage 5 program data, as well as FEMA API data for real-time tracking of disaster events. 

## Extract

The ```extract``` directory includes tools for retrieving HUD-related award and transaction data from the USAspending API. These scripts make nested API calls to gather detailed information across multiple endpoints, enriching the dataset with granular funding details.

## Utility

The ```utility``` directory houses general-purpose tools for processing structured JSON data—typically returned from API responses—and reshaping it into relational table formats suitable for database storage. It also includes automated quality checks across datasets, identifying missing values, outliers, type inconsistencies, imbalances in categorical variables, and conformity issues.

## Dashboard
This repository supports an interactive dashboard built in Tableau to analyze the Community Development Block Grant – Disaster Recovery (CDBG-DR) program in Puerto Rico. It integrates multiple datasets to provide transparent, real-time insights into the flow, allocation, and performance of disaster recovery funds.

The dashboard brings together diverse data sources:

* HUD Grant History, Financials, and Expenditures

* PRDOH Quarterly Performance Reports (QPRs)

* Federal award data from USAspending.gov

All HUD and PRDOH datasets are unified using the Grant Number as a primary key, enabling one-to-many relationships that preserve granularity and ensure referential integrity. USAspending data, which lacks a direct grant identifier, is modeled separately but can be filtered contextually by geography and year.

This schema design allows Tableau to:

*Avoid data duplication

*Maintain native granularity

* Enable cross-source comparisons using shared dimensions (e.g., Grant Number, Year)

Key Visualizations:

* Grant Award vs Drawdown: Compare total allocated vs spent per grant

* Spending Compliance Over Time: Track annual compliance against required spending targets

* Performance by Activity/Grantee: Identify underperforming grants or concentrated expenditures

* USAspending Side-by-Side: Visualize federal obligations in parallel with HUD drawdowns

* Monthly Trends in Obligations and Subawards

* Top Grants & Subrecipients by Amount

* Use of Funds by Object Class

* Spending Gaps by Grant

## Findings


Data for Equitable Results: Tracking CDBG Funds in Puerto Rico is a data analytics capstone project conducted in collaboration with Sembrando Sentido, aiming to address transparency and accountability issues in the allocation of Community Development Block Grant – Disaster Recovery (CDBG-DR) funds. Following the devastation caused by Hurricanes Irma and Maria in 2017, the federal government allocated over $91 billion for recovery efforts, including approximately $2.9 billion specifically for Puerto Rico’s Repair, Reconstruction, and Relocation (R3) housing program. 

Despite substantial funding, only 6% of eligible homes had been rebuilt by mid-2021, revealing inefficiencies, bureaucratic delays, and data transparency gaps. Existing research highlights fragmented data sources and inadequate real-time monitoring, leading to ineffective resource distribution. This project addresses these issues by integrating transaction-level data from HUD's Monthly CDBG-DR Grant Financial and History Reports, USAspending.gov's Federal Awards and Transactions, and the Puerto Rico Department of Housing's Quarterly Performance Reports into a comprehensive analytical framework. 

A relational database schema was developed to unify these datasets, enabling streamlined financial tracking and comparative analysis across disaster recovery programs. Analytical techniques such as geospatial clustering, regression analysis, and linear optimization models were employed to identify patterns, predict funding needs, and suggest optimal resource allocation strategies. Interactive dashboards built exclusively in Tableau facilitate real-time monitoring and visualization of fund distributions and project outcomes. The analysis identified critical inefficiencies in fund allocation and provided actionable insights for policy improvements. 

The resulting data infrastructure is scalable and replicable, designed to enhance governmental decision-making and nonprofit oversight, and ensure equitable aid distribution. This framework can be adapted to other disaster-affected regions, significantly contributing to transparent, effective, and accountable management of public recovery resources.

## Dashboards
The dashboards are hosted on public Tableau instances for different types of dashboards 
* [R3 Map](https://public.tableau.com/app/profile/biswajith.yetukuri/viz/R3_17458994342260/Dashboard1)
* [R3 Contracts and Managers](R3 Contracts and Managers)
* [Quaterly Reports](https://public.tableau.com/app/profile/sai.dhanush.sreedharagatta5960/viz/Quaterlyreportsdashboard/Dashboard1?publish=yes)
* [HUD](https://public.tableau.com/app/profile/sai.dhanush.sreedharagatta5960/viz/HUDDashboard/Dashboard1?publish=yes)
* [USA Spending](https://public.tableau.com/app/profile/sai.dhanush.sreedharagatta5960/viz/USASpending_1/Dashboard1?publish=yes)
* 
