library(RPostgreSQL)
library(DBI)
library(here)
library(dplyr)



##*********** IMPORTATION MULTIPLE ***************#########

files<- list.files(here("P41_2020_2021"))
files

#compte le nombre de fichiers present dans le document
nb_files <- length(files)
nb_files

#creation d'un vecteur vide pour recuperer les entetes
data_names <- vector("list",length=nb_files)

#pour recuperer les entetes
for (i in 1 : nb_files) {
  data_names[i] <- strsplit(files[i], split=".csv")
}


# importe les données et les associer aux entetes 
for (i in 1:nb_files) {
  assign(data_names[[i]], 
         read.csv2(paste(here("P41_2020_2021", files[i]) ))
  )  
  
  
}

#********************* recuperer les variables de l'objet Data de l'environnement global******##

#Blanka<- function(envir = .GlobalEnv){
 # objects<-ls(envir)
  #return(objects)
#}

#Blanka()



Blanka<-ls(envir = .GlobalEnv)
Blanka

#########  CONNEXION A LA BASE DE DONEES   ############

User='postgres'
mot_passe='dage'
host_db='localhost'
port_db=5432
dbname='daphne_dev'

con = DBI::dbConnect(RPostgres::Postgres(), user=User, password=mot_passe,
                     host=host_db, port=5432, dbnam = dbname)
dbGetQuery(con, "SELECT version()")


####### *************** IMPORTATION DES TABLES DE LA BASE DE DONNEES *********#########

# Taxo
taxo1<-dbGetQuery(con, "SELECT taxo_id, taxo_code FROM public.taxo")
taxo1

#wday
ws1<-dbGetQuery(con, "SELECT wscode, wsname FROM public.ws")
ws1

# exp_unit
exp_unit_db<- dbGetQuery(con, "SELECT unit_code, exp_unit_id, trial_code FROM public.exp_unit")
exp_unit_db

# recuperer le trial code du projet sur lequel on est à l'instant t=0
trial
jointure0<- merge(trial, exp_unit_db, by="trial_code")
jointure0

#jointure1<- merge(jointure0, itk, by="unit_code")

#--- suppression des colonnes inutiles   ----#
#itk<- select(jointure1, -trial_description, -trial_code,  -unit_code)

# Accession
accession1<-dbGetQuery(con, "SELECT accession_code, accession_id FROM public.accession")

# lot 
lot1<- dbGetQuery(con, "SELECT lot_id, lot_code  FROM public.lot")
lot1

#factor_level
factor_level1<- dbGetQuery(con, "SELECT factor_level_id, factor_level  FROM public.factor_level")
factor_level1


# recuperer le nom des table de la base de données
#result<- dbGetQuery(con, "SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
#result


#####################******** TRAITEMENT ET SAUVEGARDE ####################

 for(fichier in Blanka){
   
   #---------------------------------------------------------------------Scale ---------------------------------------------------------###
   if(fichier=='Scale'){
     scale<-Scale
     
     # Enregistrer dans la base de données
     dbWriteTable(con, "scale", scale
                  , append=TRUE,overwrite=FALSE)
     #---------------------------------------------------------------------Method ---------------------------------------------------------###
   }else if(fichier=='Method'){
     method<- Method
     
     # Enregistrer dans la base de données
     dbWriteTable(con, "method", method, append=TRUE,overwrite=FALSE)
     
     #---------------------------------------------------------------------Entity ---------------------------------------------------------###
   }else if(fichier=='Entity'){
     entity<- Entity
     
     # Enregistrer dans la base de données
     dbWriteTable(con, "entity", entity, append=TRUE,overwrite=FALSE)
     
     #---------------------------------------------------------------------Target ---------------------------------------------------------###
   }else if(fichier=='Target'){
     target<- Target
     # Enregistrer dans la base de données
     dbWriteTable(con, "target", target, append=TRUE,overwrite=FALSE)
     #---------------------------------------------------------------------Trait ---------------------------------------------------------###
   }else if(fichier=='Trait'){
     trait<- Trait
     
     # renommer une colonne trait_entity_code
     trait<-trait %>%rename(trait_entity = trait_entity_code)
     
     # renommer une colonne target_name
     trait<-trait %>%rename(target_name = trait_target_name)
     
     # Enregistrer dans la base de données
     dbWriteTable(con, "trait", trait, append=TRUE,overwrite=FALSE)
     
     #--------------------------------------------------------------------- Variable ---------------------------------------------------------###
   }else if(fichier=='Variable'){
     variable<-Variable
     
     # Enregistrer dans la base de données
     dbWriteTable(con, "variable", variable, append=TRUE,overwrite=FALSE)
     
     #---------------------------------------------------------------------Factor ---------------------------------------------------------###
   }else if(fichier=='Factor'){
     factor<-Factor
     
     # Enregistrer dans la base de données
     dbWriteTable(con, "factor", factor, append=TRUE,overwrite=FALSE)
     
     #---------------------------------------------------------------------Factor_Level ---------------------------------------------------------###
   }else if(fichier=='Factor_Level'){
     factor_level<- Factor_Level
     
     # Enregistrer dans la base de données
     dbWriteTable(con, "factor_level", factor_level, append=TRUE,overwrite=FALSE)
     
     #---------------------------------------------------------------------Taxo ---------------------------------------------------------###
   }else if(fichier=='Taxo'){
     taxo<-Taxo
     
     # Enregistrer dans la base de données
     dbWriteTable(con, "taxo", taxo, append=TRUE,overwrite=FALSE)
     
     #---------------------------------------------------------------------Accession ---------------------------------------------------------###
   }else if(fichier=='Accession'){
     accession<-Accession
     
     # jointure sur les deux tables
     jointure3<- merge(accession, taxo1, by='taxo_code' )
     
     # suppression de la colonne inutile
     accession<- select(jointure3, -taxo_code)
  
        
     # Enregistrer dans la base de données
     dbWriteTable(con, "accession", accession, append=TRUE,overwrite=FALSE)
     
     #---------------------------------------------------------------------  LOT ---------------------------------------------------------###
     
   }else if(fichier=='seedlot'){
     lot<-seedlot
     
     lot<- lot %>%rename(lot_code_partner =lot_partner_code)
     
     # jointure
     jointure4<- merge(lot, accession1, by = 'accession_code')
     
     lot<- select(jointure4, -accession_code)
   
        #---------------------------------------------------------------------WS ---------------------------------------------------------###
   }else if(fichier=='weather_station'){
     #renommer le fichier 
     ws<-weather_station
     
     # sauvegarde vers la base de données
     dbWriteTable(con, "ws", ws, append=TRUE,overwrite=FALSE)
   
     #-----------------------------------------------------------------TRIAL ----------------------------------------------------------------###  
   }
   #else if(fichier=='trial'){
     #renommer le fichier 
     #trial<-factor_trial
     
     #--- suppression d'une colonne inutile wsname  ----#
     #trial<- select(trial, -factor, -factor_level)
     
     
     # sauvegarde vers la base de données
     #dbWriteTable(con, "trial", trial, append=TRUE,overwrite=FALSE)
     
     #-----------------------------------------------------------------EXP UNIT ---------------------------------------------------------------### 
  # }else if(fichier=='exp_unit'){
     #renommer le fichier 
     #exp_unit<-design
     
     # renommer une colonne
     #exp_unit<-exp_unit %>%rename(unit_depth = profondeur)
     
     # sauvegarde vers la base de données
    # dbWriteTable(con, "exp_unit", exp_unit, append=TRUE,overwrite=FALSE)
     
     #-----------------------------------------------------------------WS TRIAL ------------------------------------------------------------### 
   #}else if(fichier=='weather_station_trial'){
     #renommer le fichier 
    # ws_trial<-weather_station_trial
     
     # renommer une colonne
    # ws_trial<-ws_trial %>%rename(wsname = name_station)
     
     # Faire la jointure pour recuprer la PK de ws
     
     #jointure<-merge(ws_trial, ws1, by ="wsname")
     #jointure
     
     
     #Supprimer la colonne inutile
     #ws_trial <- select(jointure, -wsname)
    # ws_trial
     
     # Enregistrer dans la base de données
     #dbWriteTable(con, "ws_trial", ws_trial, append=TRUE,overwrite=FALSE)
     
      
     #-----------------------------------------------------------------WEATHER DAYS ------------------------------------------------------------### 
   #}
    else if (fichier=='weather_day'){
     
     # renommer le fichier
     wd<-weather_day
     wd
     # renommer une colonne
     wd<-wd %>%rename(wsname = nom_station)
     wd<-wd %>%rename(weatherdate = date)
     wd
     # Faire la jointure pour recuprer la PK
     
     jointure<-merge(wd, ws1, by ="wsname")
     jointure
     
     #--- suppression d'une colonne inutile wsname  ----#
     weather_day <- select(jointure, -wsname)
     weather_day
     
     
     ##--- enregistrer dans la base de données ----####
     dbWriteTable(con, "weather_day", weather_day, append=TRUE,overwrite=FALSE)
     #-----------------------------------------------------------------lot_UNIT ------------------------------------------------------------###  
  
  }else if(fichier=='seedlot_unit'){
    # renommer le fichier
    lot_unit<-seedlot_unit
    
    # renommer une colonne
    lot_unit<-lot_unit %>%rename(sowing_date = gen_starting_date)
     
    # renommer une colonne
    lot_unit<-lot_unit %>%rename(ending_date = gen_ending_date)
    lot_unit
    
    #jointure entre deux tables
    jointure2<-merge(lot_unit, jointure0, by ="unit_code")
    jointure2
    
    #jointure entre deux tables
    lot_unit<-merge(jointure2, lot1, by ="lot_code")
    lot_unit
    #--- suppression des colonnes inutiles   ----#
    lot_unit <- select(lot_unit, -unit_code, -lot_code, -trial_description, -trial_code)
    lot_unit
    
    
     dbWriteTable(con, "lot_unit", lot_unit, append=TRUE,overwrite=FALSE)
    
    #-----------------------------------------------------------------FACTOR_UNIT ------------------------------------------------------------###   
  }else if(fichier =='factor_unit'){
    
    
    #jointure entre deux tables
    
    
    jointure<-merge(factor_unit, jointure0, by ="unit_code")
    jointure
    
    #jointure entre deux tables
    jointure1<-merge(jointure, factor_level1, by ="factor_level")
    jointure1
    
    #--- suppression des colonnes inutiles   ----#
    factor_unit <- select(jointure1, -unit_code, -factor, -trial_description, -trial_code, -factor_level)
    factor_unit
    
     
     dbWriteTable(con, "factor_unit", factor_unit, append=TRUE,overwrite=FALSE)
    
    
    #-----------------------------------------------------------------ITK ------------------------------------------------------------### 
  }else if(fichier=='itk'){
     
      ## renommmer le fichier pour éviter que ce soit les données de la bd qui soient pris en compte plutot que ceux du fichier#### 
     
      itk
     
     # faire une jointure pour recupérer les id de la table mère 
     
    jointure1<- merge(jointure0, itk, by="unit_code")
    jointure1
     
     ############ suppression d'une colonne inutile unit_code  ########################
    itk<- select(jointure1, -trial_description, -trial_code,  -unit_code)
    itk
     
     ######## enregistrer dans la base de données ########
     dbWriteTable(con, "itk", itk, append=TRUE,overwrite=FALSE)
     #-----------------------------------------------------------------AUTRES TABLES DE LA LISTE ------------------------------------------------------------### 
   }else {
     
     message(paste0("table ", fichier, "inconnue de notre base de données"))
   }
   
 }


###########---------------DECONNEXION------------- ########

dbDisconnect(con)

