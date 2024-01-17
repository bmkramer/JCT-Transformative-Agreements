library(tidyverse)
library(janitor)


#define function to write to file
toFile <- function(data, type, path) {
  filename <- paste0(path, "/jct_data_", type, "_", date, ".csv")
  write_csv(data, filename)
}

#define function to to extract data for each TA
#include tryCatch to deal with errors when URL does not resolve
#and report on issues with data types that raise warnings
getData <- function(data_url, esac_id_unique, ...) {
  tryCatch(
    {
      df_data <- read_csv(data_url,
                          na = na_defined,
                          col_types = cols(
                            #specify type for all columns to prevent downstream matching errors
                            `Journal Name` = col_character(),
                            `ISSN (Print)` = col_character(),
                            `ISSN (Online)` = col_character(),
                            `Journal First Seen` = col_date(format = ""),
                            `Journal Last Seen` = col_date(format = ""),
                            `Institution Name` = col_character(),
                            `ROR ID` = col_character(),
                            `Institution First Seen` = col_date(format = ""),
                            `Institution Last Seen` = col_date(format = ""))) %>%
        janitor:::clean_names() %>%
        mutate(esac_id_unique = esac_id_unique)
      
      df_data_journals <- df_data %>%
        select(esac_id_unique, 1:5) %>%
        filter(!is.na(journal_name)) #remove empty rows
      
      df_data_institutions <- df_data %>%
        select(esac_id_unique, 6:9) %>%
        filter(!(is.na(institution_name) & is.na(ror_id))) #remove empty rows
      
      res <- list(data_journals = df_data_journals,
                  data_institutions = df_data_institutions)
      
      
      return(res)
    },
    
    error = function(cond) {
      
      message(paste0(esac_id_unique, " caused an error - check whether URL resolves"))
      # Choose a return value in case of error
      return(NULL)
    },
    warning = function(cond) {
      
      message(paste0(esac_id_unique, " caused a warning - check conflicting data types in online spreadsheet (will be imported as NA)"))
      # Choose a return value in case of warning
      return(NULL)
    },
    finally={
      #prevent warning messages about unused connections (where URLs don't resolve)
      closeAllConnections()
    }
  )    
}


#-------------------------------------------------
# set date
date <- Sys.Date()
# create output directory
path <- file.path("data", date)
dir.create(path)

#read in csv with TAs from JCT
url <- "https://docs.google.com/spreadsheets/d/e/2PACX-1vStezELi7qnKcyE8OiO2OYx2kqQDOnNsDX1JfAsK487n2uB_Dve5iDTwhUFfJ7eFPDhEjkfhXhqVTGw/pub?gid=1130349201&single=true&output=csv"
df_ta <- read_csv(url)

#replace spaces in column names with underscores
df_ta <- df_ta %>% 
  janitor:::clean_names()

#create column with unique esac_ids
#more efficient than using both columns esac_id and relationship
df_ta <- df_ta %>%
  group_by(esac_id) %>%
  mutate(esac_id_unique = if(n( ) > 1) {
    paste(esac_id, row_number( ), sep = "_")
    } else {esac_id}) %>%
  ungroup() %>%
  select(esac_id_unique, everything())


#run function to extract data for each TA
#errors and warnings will include esac_id_unique to check and correct where needed
df <- df_ta
#specify na_defined for strings to consider NA (from previous processing error messages)
#na_defined <- c("", "NA") #read_csv default
na_defined <- c("", "NA", "Valid From", "OA", ".", "Open Access", "Premier")

#for troubleshooting
#df <- df_ta[1:10,]
df_res <- pmap(df, getData) %>%
  set_names(df$esac_id_unique)

#run 2024-01-17 with expanded na_defined
#errors (all NULL in list)
#none
#warnings (all NULL in list)
#wiley2021vsnu - Journal first seen misformed


#identify strings to consider as missing values and rerun
#always also include "")
#na_defined <- c("", "Valid From", "OA", ".", "Open Access", "Premier)

#Consider to replace invalid dates (see procedure below) or leave empty -> leave empty for now

#manual fix for cases where dates not formatted correctly in first row 
#id <- "cam2020kemoe_1" #copy id from warning message
#id <- "eme2020crui_3" #copy id from warning message
#date_corrected <- as.Date("2022-11-18") #copy date from online spreadsheet (link in df_ta)

#df_res[[id]][["data_institutions"]][1,4] <- date_corrected
#df_res[[id]][["data_journals"]][1,5] <- date_corrected

#replace malformed dates in wiley2021vsnu (20240117)
id <- "wiley2021vsnu"
df_retry <- df %>%
  filter(esac_id_unique == id)
#temporarily replace `Journal First Seen` = col_character in getData,
#rerun 
df_res_retry <- pmap(df_retry, getData) %>%
  set_names(df_retry$esac_id_unique)
#replace date in df
df_new <- df_res_retry[[id]][["data_journals"]] %>%
  mutate(journal_first_seen = str_replace(journal_first_seen, "20223", "2023")) %>%
  mutate(journal_first_seen = as.Date(journal_first_seen))
#replace df in list
df_res_retry[[id]][["data_journals"]] <- df_new
#replace entry in list
df_res[[id]] <- df_res_retry[[id]]

#as precaution, also save list as RDS object
#saveRDS(df_res, "data/df_res.RDS")

#collate data for journals and institutions
df_journals <- map_dfr(df_res, "data_journals")
df_institutions <- map_dfr(df_res, "data_institutions")

----------------------------------------------------------
#fix typographical errors in datasets

df_journals <- df_journals %>%
  #capitalize all ISSNs (x -> X)
  mutate(issn_print = toupper(issn_print),
         issn_online = toupper(issn_online)) %>%
  #harmonize all NAs for proper import in GBQ
  mutate(issn_online = case_when(
    issn_online == "NULL-NULL" ~ NA_character_,
    issn_online == "#N/A" ~ NA_character_,
    TRUE ~ issn_online))

#deduplicate journals and institutions lists
df_journals <- df_journals %>%
  distinct()

df_institutions <- df_institutions %>%
  distinct()

#NB General (base r) approach to identify duplicates: df1[duplicated(df1),]


#write to file
toFile(df_ta, "ta", path)
toFile(df_journals, "journals", path)
toFile(df_institutions, "institutions", path)


