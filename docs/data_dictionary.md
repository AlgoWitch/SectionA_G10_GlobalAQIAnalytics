# Data Dictionary - Global Air Quality Analytics

This document defines the schema for the cleaned air quality dataset (`clean_air_data.csv`).

## Dataset Overview
- **Source**: Global Air Quality Monitoring (Raw)
- **Timeframe**: 2000 - 2025
- **Unit of Observation**: City-Year

## Column Definitions

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| **country** | Object | Standardized name of the country (lowercase). |
| **city** | Object | Standardized name of the city (lowercase). |
| **year** | Integer | Year of observation (Filtered for 2000 – 2025). |
| **aqi** | Float | Air Quality Index (Higher = More polluted). |
| **pm2.5** | Float | Particulate Matter < 2.5 micrometers. |
| **pm10** | Float | Particulate Matter < 10 micrometers. |
| **deforestation_rate_%** | Float | % change in local forest cover removal. |
| **afforestation_rate_%** | Float | % change in local tree planting efforts. |
| **vehicles_increase_%** | Float | Annual growth rate of registered vehicles. |
| **industries_increase_%** | Float | Annual growth rate of new industrial units. |
| **env_budget_million_usd** | Float | Local government environmental spending (USD Millions). |
| **population_density_per_sqkm** | Float | Number of people per square kilometer. |
| **co2_emissions_mt** | Float | Carbon Dioxide emissions in Million Tonnes. |
| **green_space_ratio_%** | Float | Percentage of urban area covered by vegetation. |
| **avg_life_expectancy_index** | Float | Normalized index for life expectancy (based on local health data). |

## Data Engineering Rules
- **Outlier Multiplier**: 1.5x Interquartile Range (IQR).
- **Imputation Logic**: Country-wise Median replacement with fall-back to Global Median.
- **Scaling**: Budget values >10,000 scaled down by 100 for consistency.
