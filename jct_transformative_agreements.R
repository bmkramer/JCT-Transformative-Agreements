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
#for troubleshooting
#df <- df_ta[1:100,]
df_res <- pmap(df, getData) %>%
  set_names(df$esac_id_unique)

#run 2022-12-09
#errors: 
#eme2020crui_5 - file deleted

#warnings:
#tf2020unit_2 - note in last seen column - no problem - done
#cam2020kemoe_1 - first date in both j/i not formatted as date - replace - done
#cam2020kemoe_2 - ditto
#cam2020kemoe_3 - ditto
#eme2020kemoe - note in last seen column - no problem - done

#manual fix for cases where dates not formatted correctly in first row 
id <- "cam2020kemoe_1" #copy id from warning message
id <- "eme2020crui_3" #copy id from warning message
date_corrected <- as.Date("2022-11-18") #copy date from online spreadsheet (link in df_ta)

df_res[[id]][["data_institutions"]][1,4] <- date_corrected
df_res[[id]][["data_journals"]][1,5] <- date_corrected

#as precaution, also save list as RDS object
saveRDS(df_res, "data/df_res.RDS")

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

#General (base r) approach to identify duplicates: df1[duplicated(df1),]


#write to file
toFile(df_ta, "ta", path)
toFile(df_journals, "journals", path)
toFile(df_institutions, "institutions", path)


