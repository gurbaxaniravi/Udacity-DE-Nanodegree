#!/usr/bin/python
# -*- coding: utf-8 -*-


class SqlQueries:
    """
    This class has all the SQL Queries that will be used for the entire Data Pipeline
    """

    
    create_table_staging_global_cases = \
        """
        CREATE TABLE IF NOT EXISTS public.staging_global_cases (
            country_region varchar(256),
            province_state varchar(256),
            county varchar(256),
            fips varchar(256),
            date timestamp,
            case_type varchar(256),
            cases numeric(18,0),
            long real,
            lat real,
            ISO3166_1 varchar(256),
            ISO3166_2 varchar(256),
            difference numeric(18,0),
            last_updated_date timestamp,
            last_reported_flag boolean
        );
        """

    create_table_staging_databank_demographics = \
        """
        CREATE TABLE IF NOT EXISTS public.staging_databank_demographics (
                ISO3166_1 varchar(256),
                ISO3166_2 varchar(256),
                fips varchar(256),
                long real,
                lat real,
                state varchar(256),
                county varchar(256),
                total_population numeric(18,0),
                total_male_population numeric(18,0),
                total_female_population numeric(18,0),
                country_region varchar(256)
            );
        """
    
    create_table_countrydemographics_dim = \
        """
        CREATE TABLE IF NOT EXISTS public.countrydemographics_dim (
                country_code varchar(256),
                country_region varchar(256),
                total_population numeric(18,0),
                total_male_population numeric(18,0),
                total_female_population numeric(18,0)
        );
        """
    
    create_table_date_dim = \
        """
        CREATE TABLE IF NOT EXISTS public.date_dim (
                date date,
                day int4,
                month varchar(256),
                year int4,
                dayofweek varchar(256),
                CONSTRAINT date_pkey PRIMARY KEY (date)
        );
        """
    
    create_table_globalcases_fact = \
        """
        CREATE TABLE IF NOT EXISTS public.globalcases_fact (
                country_code varchar(256),
                date date,
                case_type varchar(256),
                cases numeric(18,0)
        );
        """

    countrydemographics_dim_table_insert = \
        """
        SELECT 
            ISO3166_1,
            country_region,
            total_population,
            total_male_population,
            total_male_population
        FROM public.staging_databank_demographics
        """
    
    globalcases_fact_table_insert = \
        """
        SELECT 
            ISO3166_1,
            trunc(date) as date,
            case_type,
            SUM(cases)
        FROM public.staging_global_cases
        GROUP BY ISO3166_1,date,case_type
        """
    
    date_dim_table_insert = \
        """
        SELECT
            DISTINCT date,
            extract(day from date),
            extract(month from date),
            extract(year from date),
            extract(dayofweek from date)
        FROM globalcases_fact;
        """
