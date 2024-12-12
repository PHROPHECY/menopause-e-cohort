pkgs <- c(
  # load these first to not highjack the other packages
"knitr",
"tidyverse",
"scales",
"ggplot2",
"dplyr",
"lubridate",
"RODBC",
"labeling",
"tibble",
"svDialogs",
"stringi",
"httr",
"getPass",
"keyring",
"janitor",
"kableExtra",
"SAILDBUtils",
"data.table",
"gtsummary"

)

install_if_missing <- function(package){
  if (!require(package, character.only = TRUE)) {
    install.packages(package, dependencies = TRUE)
    library(package, character.only = TRUE)
  }
}

for (pkg in pkgs) {
  suppressWarnings(
    suppressPackageStartupMessages(
      install_if_missing(pkg)
    )
  )
}

uid <- readline(prompt = "Enter SAIL Username: ")
pwd <- readline(prompt = "Enter SAIL Password: ")

conn <- RODBC::odbcConnect(dsn = "PR_SAIL", 
													uid = uid, 
													pwd = pwd, 
													case = "toupper")
