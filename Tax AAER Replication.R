#####################
#██     ██ █████▄ █████▄  ▄████▄ █████▄  ██ ██████ ▄█████ 
#██     ██ ██▄▄██ ██▄▄██▄ ██▄▄██ ██▄▄██▄ ██ ██▄▄   ▀▀▀▄▄▄ 
#██████ ██ ██▄▄█▀ ██   ██ ██  ██ ██   ██ ██ ██▄▄▄▄ █████▀ 
#####################
library(RPostgres)
library(RPostgreSQL)
library(tidyr)
library(stargazer)
library(broom)
library(tidymodels)
library(stringr)
library(glmnet)
library(recipes)
library(xtable)
library(tikzDevice)
library(ggplot2)
library(data.table)
library(lubridate)
library(knitr)
library(ebal)
library(fuzzyjoin)
library(WeightIt)
library(DescTools)
library(plm)
library(openxlsx)
library(dplyr)
library(topicmodels)
library(tm)
library(tidytext)
library(quanteda)
library(conflicted)
library(reshape2)
conflict_prefer("lag", "dplyr")
conflict_prefer("lead", "dplyr")
conflict_prefer("filter", "dplyr")
conflict_prefer("year", "lubridate")
conflict_prefer("month", "lubridate")
setwd("C:/Users/andre/OneDrive - University of Oklahoma/3. Research/Tax AAER Replication/Tax AAER Replication")
#https://patorjk.com/software/taag/#p=display&f=ANSI+Compact&t=%0A&x=none&v=4&h=4&w=80&we=false


#Create a connection to WRDS (Wharton Research Data Service) with Compustat data
WRDS<- dbConnect(RPostgres::Postgres(),
                 host='wrds-pgdata.wharton.upenn.edu',
                 port=9737,dbname='wrds',
                 user=Sys.getenv("wrds_user"),
                 password=Sys.getenv("wrds_pass"))


#####################
#████▄  ▄████▄ ██████ ▄████▄ 
#██  ██ ██▄▄██   ██   ██▄▄██ 
#████▀  ██  ██   ██   ██  ██ 
#####################
#Get Financial data from Compustat Fund Annual
#█████████ Commenting out cuz a prior save will load at the bottom
#█comp_funda <- dbSendQuery(WRDS,"select gvkey, cusip, cik, fyear, fyr, sich, tic, at, sale, prcc_f, ib, csho, seq, pstk, ceq, 
#█                                  aqs, rca, dltt, dlc, pifo, txfo, txdfo, datadate, xrd, ppent, txbcof, tlcf, txndba, txndbl,  
#█                                  txtubend AS UTP, txtubtxtr AS UTP_perm, fdate, pdate, apdedate
#█                                from comp.funda
#█                                where ((datadate between '1995-01-01' and '2003-12-31')
#█                                  and (fyear between '1995' and '2003')
#█                                  and (indfmt = 'INDL')
#█                                  and (datafmt = 'STD')
#█                                  and (indfmt = 'INDL')
#█                                  and (consol = 'C')
#█                                  and (popsrc = 'D')
#█                                  and (gvkey IS NOT NULL)
#█                                  and (fyear IS NOT NULL)
#█                                  and (at IS NOT NULL)
#█                                  and (sale IS NOT NULL)
#█                                  and (dltt IS NOT NULL)
#█                                  ) ")
#█funda <- dbFetch(comp_funda)
#█dbClearResult(comp_funda)
#█save(funda, file = "funda.RData")
load("funda.RData")


# gvkey is the firm identifier in Compustat
# CUSIP is another identifier, which identifies the primary security (likely the common stock) of the firm
# fyear is the fiscal year
# fyr is the month of the fiscal year-end
# sich is the historical SIC code
# ni is net income
# tic is the ticker symbol
# ib is income before extraordinary items
# csho is common shares outstanding
# seq is stockholder's equity
# pstk is preferred stock
# ceq is value of common equity (for MTB)
# aqs is acquisition expenditures
# rca is restructuring expenditures
# dltt is long-term debt (for Leverage)
# dlc is current debt (for Leverage)
# oancf is operating cash flow (operating activities net cash flow)
# at is total assets (assets total)
# sale is total operating revenue
# prcc_f is the closing stock price of the fiscal year
# csho is the number of common shares outstanding
# pifo is pretax income from foreign operations
# txfo is Foreign Tax
# txdfo is Deferred Foreign Tax
# txbcof is tax benefit of carryforwards (NOL carryforward tax-adjusted)
# tlcf is tax loss carryforward (NOL carryforward amount)
# txndba is Deferred Tax Assets
# txndbl is Deferred Tax Liabilities
# txtubend is ending balance of Uncertain Tax Positions
# txtubtxtr is permanent uncertain tax positions 

# datadate is a year variable formatted as a date
# datafmt, indfmt, consol, and popsrc are flags that indicate the data format, industry format, consolidation, and population source
# (these just make sure our data is structured to compare apples to apples, like making sure the data is in the same currency)



#Get AAER data from Audit Analytics
#█████████ Commenting out cuz a prior save will load at the bottom
audit_AAER <- dbSendQuery(WRDS,"select *
                                    from audit_acct_os.feed91_aaer
                                    where (first_release_date between '1995-01-01' and '2003-12-31') 
                                    ")
AAER <- dbFetch(audit_AAER)
dbClearResult(audit_AAER)
save(AAER, file = "AAER.RData")
load("AAER.RData")
write.csv(my_data, "C:/Users/YourName/Documents/output.csv", row.names = FALSE)


# stype is the type of segment (business or geographic)
# sales is the amount of revenue attributed to this segment
# snms is the name of the segment



audit_link <- dbSendQuery(WRDS,"select *
                                    from audit_acct_os.f91_feed91_respondent
                                    where cik IS NOT NULL
                                    ")
link <- dbFetch(audit_link)
dbClearResult(audit_link)
save(link, file = "link.RData")
load("link.RData")



# Inspect the AAER table structure first
str(AAER)
# Most likely columns: aaer_no, release_date, summary/allegations text fields,
#                      respondent_name, etc.

str(link)  
# Most likely: aaer_no, cik, company_name, role (respondent vs co-respondent)

# Filter to fraud-related earnings overstatements
# Adjust the column names below to match your actual schema
AAER_filtered <- AAER %>%
  filter(release_date >= as.Date("1996-01-01"),
         release_date <= as.Date("2002-06-30")) %>%
  # Look for "fraud" language - adjust field name
  filter(grepl("fraud", summary, ignore.case = TRUE) |
           grepl("fraud", allegations, ignore.case = TRUE)) %>%
  # Look for overstatement language
  filter(grepl("overstat|inflat|fictitious|false revenue|premature|nonexistent",
               summary, ignore.case = TRUE)) %>%
  # Deduplicate at the firm level (paper notes 88 dupes in their 228)
  distinct()

# Link to firms via CIK
AAER_firms <- AAER_filtered %>%
  inner_join(link, by = "aaer_no") %>%   # whatever the join key actually is
  filter(!is.na(cik)) %>%
  # If a firm has multiple AAERs, collapse to one record
  group_by(cik) %>%
  summarise(first_aaer_date = min(release_date),
            aaer_text_concat = paste(summary, collapse = " | "),
            .groups = "drop")


#####################
# ▄▄▄▄ ▄▄▄▄▄  ▄▄▄▄ ▄▄   ▄▄ ▄▄▄▄▄ ▄▄  ▄▄ ▄▄▄▄▄▄ ▄▄▄▄ 
#███▄▄ ██▄▄  ██ ▄▄ ██▀▄▀██ ██▄▄  ███▄██   ██  ███▄▄ 
#▄▄██▀ ██▄▄▄ ▀███▀ ██   ██ ██▄▄▄ ██ ▀██   ██  ▄▄██▀ 
#####################
# Group by gvkey, datadate, and stype
# Count how many unique values there are for BUSSEG and GEOSEG
# Turn those into separate columns (if no BUSSEG or GEOSEG, set the value to 1)
# Formatting to make it look pretty
segdtail2 <- segdtail %>%
  group_by(gvkey, datadate, stype) %>%
  summarise(sid_count = n_distinct(sid)) %>%
  pivot_wider(names_from = stype, values_from = sid_count, values_fill = 1) %>%
  ungroup()
#██████ What to do about segments where sales are 0 or NA description is also NA? Remove?
#██████ What about negative sales or "Eliminations" (typically sid=99) or "Corporate" BUSSEG where sales=0? Remove?

# Add the domestic sales amount from Compustat Segments so that we can calculate the foreign sales amount in the next step
#Filters on North America and United States
segdtail3 <- segdtail %>%
  filter(snms %in% c("United States", "North America")) %>%
  select(gvkey, datadate, sales, snms)

#supposedly removes North America if there's a United States
segdtail3 <- segdtail3 %>%
  group_by(gvkey, datadate) %>%
  filter(!(any(snms == "North America") & any(snms == "United States"))) %>%
  ungroup()

#Renames sales to include segments (to avoid confusion with funda sales)
segdtail3 <- segdtail3 %>%
  rename(seg_sale = sales)

#Combines into one happy family
segdtail4 <- left_join(segdtail2, segdtail3, by = c("gvkey", "datadate"))
#██████ Why are we doing all this? To find foreign sales? Why not just use the compustat foreign sales amount? Is that a thing?


#####################