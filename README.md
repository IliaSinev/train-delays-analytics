# Analytics of Train Delays of UK Rail

- [Preface](#preface)
- [Introduction](#introduction)
- [Dataset description](#dataset-description)
- [Objective and requirements](#objective-requirements)
- [Solution](#solutiob)
    - [Technology stack](#technologies)
    - [Infrastructure as a code](#iac)
    - [Cloud infrastructure](#cloud)
    - [Data pipeline orchestration and scheduling](#orchestration)
    - [Data modelling](#datamodelling)
    - [Reporting](#report)
- [Future development](#todo)

## Preface

This repository contains the final project for the Data Engineering Zoomcamp (github.com/DataTalksClub/data-engineering-zoomcamp) - a free course organized by the DataTalks.Club (datatalks.club) community.
Course tutors: Victoria Perez Mola https://linkedin.com/in/victoriaperezmola, Sejal Vaidya https://linkedin.com/in/vaidyasejal, Ankush Khanna https://linkedin.com/in/ankushkhanna2, Alexey Grigorev https://linkedin.com/in/agrigorev. The project covers main data engineering skills taught in the course: Infrastructure as code, containerization, data lake, ETL/ELT, pipeline orchestration and scheduling, batch processing, data warehousing, reporting.

## Introduction

It was initially intended that the data set would consist of multiple file with record count at least on a level of thousands. Thus, a monthly load can be simulated with an amount of data high enough to consider ELT instead of ETL. After thorough review of the available free data sets, UK passenger train delays data set from NetwokRail was chosen https://www.networkrail.co.uk/who-we-are/transparency-and-ethics/transparency/open-data-feeds/.

## Dataset description

There are two types of data in the dataset - ziped csv files, containing delays in minutes with the attribute codes, and a xlsx file with attribute glossary.

    ### Historic delay files

    Comma-separated files containing attribute codes (strings and integers), date/datetime attributes and delay values (float). Each file covers delays over 28/29 day span, leading to 13 files per two-year period: e.g. 2020/21 covers 13 periods starting on 1. April 2020 (encoded as 2020/21_P1) and ending on 31 March 2021 (encoded as 2020/21_P13).

    __Key attributes:__
    - EVENT_DATETIME - date and time when the event leading to the delay happened. Either in DD-MMM-YYYY HH:mm (e.g. 11-NOV-2018 17:53) or DD/MM/YYYY HH:mm (e.g. 08/12/2019 13:43) format.
    - INCIDENT_NUMBER - number of the incident (forms unique identifier together with EVENT_DATETIME)
    - PERFORMANCE_EVENT_CODE - Whether the train has been delayed or cancelled. A and M denote delays, C – is a full cancelletion, D is a diversion, F is a failure to stop, S is a scheduled cancellation and O/P are part cancellations
    - START_STANOX/END_STANOX - the location of the delay (not the incident)
    - RESPONSIBLE_MANAGER - who within the industry is responsible for the delay
    - OPERATOR_AFFECTED - code of the cmpany that operates the delayed train
    - INCIDENT_REASON - the Delay Attribution Guide cause code for the incident

    __IMPORTANT__
    Delay files have schema and date/datetime representation changing.

    ### Historic-Delay-Attribution-Glossary
    xlsx file containing sheets with attribute glossary:
    - Stanox Codes
    - Period Dates
    - Incident Reason
    - Responsible Manager
    - Reactionary Reason Code
    - Performance Event Code
    - Service Group Code
    - Operator Name
    - Train Service Code

## Objective and requirements

Develop end-to-end data solution to perform advanced analysis of the train delays. Solution must meet the following requirements:
- Infrastructure provided via code (IaC approach)
- Cloud storage - data lake and data warehouse
- Create sperate pipelines for historic data and attributes. Historic data pipeline must run every month, pipeline that ingests attributes will be triggered ad-hoc.
- Data transformations must be performed using software engineering principles (data documentation, testing) and in the way that data analysts can easily understand and modify transformations.
- Create interactive report.

## Solution


Data and Attributes glossary:
https://www.networkrail.co.uk/who-we-are/transparency-and-ethics/transparency/open-data-feeds/

Stations attreibutes:
https://dataportal.orr.gov.uk/statistics/infrastructure-and-emissions/rail-infrastructure-and-assets

KPIs:

Incident_JPIP_Category vs. SUM Minutes (Heatmap)
 
Operator name vs. 
