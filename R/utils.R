# API CAMMESA
# ===========
# Autor: rikivillalba@gmail.com


resumen.buscar <- function(
    nemo = "DTE_UNIF",
    folder = "~/../OneDrive - Enel Spa/cammesa/Downloads",
    search.pattern = function(version, docId, attchId) {
      sprintf("^(%s)_(%s)_(%s)$", version, docId, attchId)})
{

  regex <- list(
    version = "\\d{8}T\\d{4}",
    docId = "[0-9A-F]{32}"  ,
    attchId = ".*")

  #  data.table:::patterns()
  #  pattern <- "^()_()_(.*)$"

  dt.files <- dir(
    file.path(folder, nemo),
    pattern = with(regex, search.pattern(version, docId, attchId)))

  # gsub("([\\\\{}()+*?^$|.])", "\\\\\\1", nemo)),
  # lista descargada

  # Obtener lista de archivos descargados de directorio
  dt.files <- regexec(pattern, files) |> regmatches(x = files) |>
    lapply(c) |> do.call(what = rbind.data.frame) |>
    setNames(c("file", "date", "id", "adjuntos_nombre")) |>
    transform(
      date = as.POSIXct(date, tz = "Etc/GMT+3", format = "%Y%m%dT%H%M"))

  # coincidencias
  dt.both <- merge(dt.files, dt.query)

  lastdate <- max(dt.both$date)

  # buscar y descargar documentos (que no existen aún)
  dt.download <- findDocumentosByNemo(
    nemo = nemo,
    fechadesde = lastdate - 30L * 86400L,
    fechahasta = Sys.time(),
    folder = folder,
    download = F)

}
