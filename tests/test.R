
# ---- TEST ----

if (F) {

  library(data.table)

  cp <- catalogoPublicaciones()
  nemos <- cp$nemo[grepl("DTE|PARTE_SEM|ESTACIONAL", cp$nemo)
                   & !grepl("UNIF|REVISTA|FUEGO|TDF", cp$nemo) ]

  docs <- lapply(nemos, findDocumentosByNemo, download.policy = "missing",
                 get.config = list(httr::timeout(5)))

}


# ----  partes mensuales viejo y nuevo ----
if (F) {

  library(data.table)

  folder <- "~\\..\\OneDrive - Enel Spa\\cammesa\\downloads"
  dir.relative <- function(f) {
    sub(normalizePath(f), "", normalizePath(dir(f, full = T, recursive = T)), fixed = TRUE) }

  sha1 <- fread(fs::path(folder,"sha1sum.csv"))
  sha1[, path := fs::path_rel(fs::path(folder, path), folder)]


  dt <- getQueryDT()

  viejo <- data.table(file = dir(file.path(folder, "viejo"), full = T))
  viejo[, adjuntos_id := sub("^\\d+T\\d+_(.*)$", "\\1", basename(file))]
  viejo[, path := fs::path_rel(fs::path_real(file), fs::path_expand_r(folder)) ]
  viejo[sha1, on = "path", sha1 := i.sha1]
  viejo[, version := sub("^(\\d+T\\d+)_.*$", "\\1", basename(file)) |>
          as.POSIXct(format = "%Y%m%dT%H%M00", tz = "Etc/GMT+3")]
  setorder(viejo, adjuntos_id, version)
  viejo[ , id_version := rowid(adjuntos_id)]

  #viejo[by = .I , , sha1:=digest::digest(file = file, algo  ="sha1")]


  nuevo <- data.table(file = dir(file.path(folder, "PARTE_SEMANAL"), recursive = T, full = T))
  nuevo[, adjuntos_id := sub("^\\d+T\\d+_[^_]+_(.*)$", "\\1", basename(file))]
  nuevo[, path := fs::path_rel(fs::path_real(file), fs::path_expand_r(folder)) ]
  nuevo[, id := sub("^\\d+T\\d+_([^_]+)_.*$", "\\1", basename(file))]
  #nuevo[by = .I, , sha1:=digest::digest(file = file, algo  ="sha1")]
  nuevo[sha1, on = "path", sha1 := i.sha1]
  nuevo[, version := sub("^(\\d+T\\d+)_.*$", "\\1", basename(file)) |>
          as.POSIXct(format = "%Y%m%dT%H%M", tz = "Etc/GMT+3")]
  nuevo <- nuevo[grep("(?i)SETM", adjuntos_id)]
  nuevo[is.na(sha1), by = .I, sha1 := digest::digest(file = file, algo = "sha1")]  # LOS QUE NO
  setorder(nuevo, adjuntos_id, version)
  nuevo[ , id_version := rowid(adjuntos_id)]

  clipr::write_clip(viejo)
  clipr::write_clip(nuevo)

  viejo[nuevo, on = "sha1", nomatch = NULL]
  nuevo[viejo, on = "sha1", nomatch = NULL]
}

x <- data.table(file=dir("~\\..\\downloads", "DEMANDA", full =T))
x[,by=.I, sha1:= digest::digest(file = file, algo = "sha1")]
x

shell.exec(nuevo[sha1 == "19879a3b10e153dfe558683a7ae24f0b5c2e05be", file])
clipr::write_clip(zip::zip_list(
  nuevo[sha1 == "19879a3b10e153dfe558683a7ae24f0b5c2e05be", file]))

shell.exec(viejo[sha1 == "a7b42a68c7a85ec33aa6893d294199a4aa8e65b1", file])
clipr::write_clip(zip::zip_list(
  viejo[sha1 == "a7b42a68c7a85ec33aa6893d294199a4aa8e65b1", file]))


# ---- esto es de los backups ----
f <- dir("~\\..\\OneDrive - Enel Spa\\cammesa\\downloads", rec =T, full =T)
f <- data.table(file = f)
f <- f[file %in% dir("~\\..\\OneDrive - Enel Spa\\cammesa\\downloads", rec =T, full =T)]
f[, by=.I, sha1 := digest::digest(file = file, algo = "sha1")]
fwrite(file = file.path("~\\..\\OneDrive - Enel Spa\\cammesa\\downloads", "sha1sum.csv"),
       f[, .(path = sub(normalizePath("~\\..\\OneDrive - Enel Spa\\cammesa\\downloads"), "",
                        normalizePath(file), fixed = T), sha1 = sha1)])


g <- dir("G:\\work\\onedrive\\cammesa\\DTE", rec =T, full =T)
g <- data.table(file = g)
g <- g[file %in% dir("G:\\work\\onedrive\\cammesa\\DTE", rec =T, full =T)]
g[, by = .I, sha1 := {
  message(file)
  digest::digest(file = file, algo = "sha1")}]
mfiles
mfiles <- dir(clipr::read_clip())
mfiles <- gsub("\\d{4}", "\\\\d{4}" , mfiles)
g[rowSums(sapply(paste0("^",mfiles,"$"), grepl, basename(file)))>0]


# buscar y borrar carpetas vacías
while (T) {
  d <- list.dirs("G:\\work\\onedrive", recursive = T)
  dl <- lapply(d, list.files)
  if (!length(unl <- d[lengths(dl)==0])) break
  else unlink(unl, recursive = T)
}

u <- g[f, on = "sha1", nomatch = NULL]
View(u)
unlink(u$file)

f[, size := file.info(file)$size]

# ---- compara dos directorios -----

f1 <- "C:\\Users\\AR30592993\\OneDrive - Enel Spa\\Envío QUANTUM 2024"
f2 <- "G:\\work\\RTI 2024\\Envío QUANTUM 2024"


l1 <- dir.relative(f1)
l2 <- dir.relative(f2)
l2[!(l2 %chin% l1)]
l1[!(l1 %chin% l2)]
