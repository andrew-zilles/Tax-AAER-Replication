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
setwd("C:/Users/andre/OneDrive - University of Oklahoma/3. Research/Firm Disclosure Under Audit Certainty/Firm-Disclosure-Under-Audit-Certainty")
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
#█                                where ((datadate between '1997-01-01' and '2014-12-31')
#█                                  and (fyear between '1997' and '2013')
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
#█                      ) ")
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



#Get Segment data from Compustat Segments Historical
#█████████ Commenting out cuz a prior save will load at the bottom
#█comp_wrds_segdtail <- dbSendQuery(WRDS,"select gvkey, sid, datadate, stype, srcdate, sales, snms
#█                                        from compseg.wrds_segmerged
#█                                        where ((datadate between '1997-01-01' and '2020-01-01')
#█                                          and (srcdate = datadate)
#█                              ) ")
#█segdtail <- dbFetch(comp_wrds_segdtail)
#█dbClearResult(comp_wrds_segdtail)
#█save(segdtail, file = "segdtail.RData")
load("segdtail.RData")

# stype is the type of segment (business or geographic)
# sales is the amount of revenue attributed to this segment
# snms is the name of the segment



#Get Price data from CRSP
#wrds_crsp_price <- dbSendQuery(WRDS,
#                                  "select permno, cusip, date, prc
#                            from crsp.msf
#                            where (date between '1997-01-01' and '2020-01-01'
#                              )
#                            ")
#crsp_prices <- dbFetch(wrds_crsp_price)
#dbClearResult(wrds_crsp_price)
# prc is the trading price at the end of the day



#Get Analyst Following from IBES
#█████████ Commenting out cuz a prior save will load at the bottom
#█IBES_analysts <- dbSendQuery(WRDS,"select cusip, fpedats, numest, fpi, ticker, oftic, measure, medest, meanest, stdev, actual, statpers
#█                                   from ibes.statsum_epsus
#█                                   where (extract(YEAR FROM fpedats) >= 1997 
#█                                        and extract(YEAR FROM fpedats) <= 2020 
#█                                        and fpi = '1'
#█                              ) ")
#█analyst_following <- dbFetch(IBES_analysts)
#█dbClearResult(IBES_analysts)
#█save(analyst_following, file = "analyst_following.RData")
load("analyst_following.RData")



# fpedats is the weirdest way I've seen them code the date value (forecast period end date)
# statpers IBES Statistical Period - the date the data was collected
# numest is the number of analysts following the firm
# fpi stands for fiscal period indicator - should be "1" for an annual forecast instead of quarterly



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