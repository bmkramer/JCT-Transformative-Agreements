library(tidyverse)
library(janitor)


#define function to write to file
toFile <- function(data, type, path) {
  filename <- paste0(path, "/jct_data_", type, "_", date, ".csv")
  write_csv(data, filename)
}

#define function to to extract data for each TA
getData <- function(data_url, esac_id_unique, ...) {
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
#TO DO add tryCatch, until then, run in chunks to prevent errors halting the function
df <- df_ta[1:100,]
df_res <- pmap(df, getData)
#df_resx <- pmap(df, getData)

df_res <- append(df_res,
                 df_resx)
rm(df_resx)

#errors: 88  (file deleted) eme2020crui_5 
#problems:
#236 note in last seen column - no problem - done
#275 first date in both j/i not formatted as date - replace - done
#276 ditto
#277 ditto
#295 note in last seen column - no problem - done

datex <- as.Date("2022-11-18")
df_resx[[1]][["data_institutions"]][1,4] <- datex
df_resx[[1]][["data_journals"]][1,5] <- datex

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


