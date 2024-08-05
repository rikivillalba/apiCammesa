#API CAMMESA

#' @import data.table
NULL

# Funciones auxiliares
parseFecha <- function(fecha, tz = "Etc/GMT+3") {
  if (inherits(fecha, "Date")) {
    # la API REST de CAMMESA toma fechas UTC con medianoche local,
    # es decir 3:00 AM UTC para fechas sin hora.
    as.POSIXct(as.character(fecha), tz = tz)
  } else {
    fcoalesce(
      as.POSIXct(fecha, format = "%Y-%m-%dT%H:%M:%OS%z", tz = tz),
      as.POSIXct(fecha, tz = tz))
  }
}

# consultar dividiendo la query en tramos mensuales
splitYM <- function(x) {
  unname(split(x, with(as.POSIXlt(x), year * 12 + mon)))
}

findDocumentosByNemo_resp  <- function(responses) {
  responses <- lapply(responses, httr::content, simplifyVector = T)
  responses <- lapply(responses, function(tbl) {
    if (!NROW(tbl)) return(NULL)
    setDT(tbl)
    #if (any(unlist(lapply(tbl$adjuntos, NROW))) == 0) {
    # linea sin adjuntos. Crea una linea fake
    stopifnot(is.list(tbl$adjuntos))
    tbl[,adjuntos := fifelse(
      unlist(lapply(adjuntos, NROW)) == 0,
      list(data.table(
        id = NA_character_, campo = NA_character_, nombre = NA_character_)),
      adjuntos)]
    #    }
    tbl <- tbl[,by = .I, as.data.table(unlist(.SD, recursive = FALSE))]
    tbl[, I := NULL]
    #    tbl[comentario == "", comentario := NA]
    #    tbl[titulo == "", titulo := NA]
    tbl[,comentario := fcoalesce(as.character(comentario), "")]
    tbl[,titulo := fcoalesce(as.character(titulo), "")]
    tbl[, version := as.POSIXct(
      sub(":..$","00", version), format = "%Y-%m-%dT%H:%M:%OS%z", tz = "UTC")]
    setnames(tbl, sub("\\.", "_", names(tbl)))

    tbl
  })
  responses
}

# ----- funciones públicas ----

#' El campo NEMO devuelto en la estructura de respuesta de este servicio es
#' utilizado como identificador de la publicación por el resto de los servicios.
#'
#' @param vigentes Publicaciones vigentes
#' "https://api.cammesa.com/pub-svc/public/catalogoPublicaciones"
#' "https://api.cammesa.com/pub-svc/public/catalogoPublicacionesVigentes"
#'
#' @return data.frame
#' @export
#'
#' @examples
catalogoPublicaciones <- function(vigentes = F) {
  url <- if (isTRUE(vigentes))
    "https://api.cammesa.com/pub-svc/public/catalogoPublicacionesVigentes"
  else
    "https://api.cammesa.com/pub-svc/public/catalogoPublicaciones"

  message("Consultando ", url, "...")
  response <- httr::GET(url)
  httr::content(response, simplifyVector = T)
}

#' Descripción de la api de cammesa
#' "https://api.cammesa.com/pub-svc/v2/api-docs"
#' @return
#' @export
#'
#' @examples
api_docs <- function() {
  httr::content(httr::GET("https://api.cammesa.com/pub-svc/v2/api-docs"),
                simplifyVector = F)
}


#' Buscar y opcionalmente descargar documentos de CAMMESA por fecha y NEMO
#'
#' @param nemo nemo de clase de documento de la API de cammesa
#' @param fechadesde fecha de consulta desde, fechas desde
#' "2010-06-01T03:00:00.000Z" fueron probadas
#' @param fechahasta fecha de consulta hasta
#' @param folder ruta base de descarga (los archivos se descargan en una
#' @param download.policy no bajar, sobreescribir o bajar sólo faltantes
#' @param queryByMonth divide la consulta en meses
#' @param tz
#' @param get.config pasado a httr::GET para la descarga de arhcivos
#'
#' @return data.frame de documentos encontrados para las fechas y nemo
#' especificados (devuelto por la llamada REST a
#' \code{findDocumentosByNemoRango})
#' @export
#' @details El período de consultas es dividido en tramos mensuales para la
#' consulta y luego se concatenan. Por experiencia eso asegura que la consulta
#' devuelva todos los archivos
#'
#' @examples
findDocumentosByNemo <- function(
    nemo = "DTE_EMISION",
    fechadesde = as.POSIXct(
      trunc(Sys.time() - 30L * 86400L, "month"), tz = tz),
    fechahasta = Sys.time(),
    tz = "America/Buenos_Aires",
    folder = "~\\..\\OneDrive - Enel Spa\\cammesa\\downloads",
    download.policy = c("none", "missing.only", "overwrite"),
    queryByMonth = TRUE, get.config = list())
{

  stopifnot(is.character(nemo) && length(nemo) == 1)
  stopifnot(is.atomic(fechadesde) && length(fechadesde) == 1)
  stopifnot(is.atomic(fechahasta) && length(fechahasta) == 1)
  download.policy <- match.arg(download.policy)

  if (queryByMonth) {
    fechas <- splitYM(seq.POSIXt(
      parseFecha(fechadesde, tz), parseFecha(fechahasta, tz), "days")) |>
      lapply(range)
    l <- length(fechas)
    fd <- as.POSIXct(unlist(lapply(fechas, `[`, 1)))
    fh <- as.POSIXct(c(unlist(lapply(fechas[-1], `[`, 1)), fechas[[l]][2]))
    fechas <- Map(f = c, fd, fh) |>
      lapply(format, "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  } else {
    fechas <- list(format(
      c(parseFecha(fechadesde[1], tz), parseFecha(fechahasta[1], tz)),
      "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
  }

  responses <- lapply(fechas, function(f) {
    message(sprintf(
      "Consultando desde %s hasta %s", f[1], f[2]))
    url <- httr::parse_url(
      "https://api.cammesa.com/pub-svc/public/findDocumentosByNemoRango") |>
      within.list(
        query <- list(fechadesde = f[1], fechahasta = f[2], nemo = nemo)) |>
      httr::build_url()
    message("request ", utils::URLdecode(url))
    httr::stop_for_status(resp <- httr::GET(url))
    resp
  })

  tmpfile <- tempfile("findDocumentosByNemo", fileext = "rds")
  saveRDS(responses, tmpfile)
  on.exit(message(gettextf("datos descargados guardados en %s", tmpfile)))

  if (download.policy != "none") {
    # simplifyVector = F no procesa las tablas (puro json mas facil)
    resp_tables <- lapply(responses, httr::content, simplifyVector = F)

    for (rjson in resp_tables) for (ai in rjson) for (adj in ai$adjuntos) {
      cat("ai=",ai$id," adj=", adj$id, "\n")
      url <- httr::modify_url(
        "https://api.cammesa.com/pub-svc/public/findAttachmentByNemoId",
        query = list(nemo = ai$nemo, docId = ai$id, attachmentId = adj$id))
      if(!dir.exists(file.path(folder, nemo)))
        dir.create(file.path(folder, nemo))
      path <- file.path(
        folder, ai$nemo, #TODO: baja en el nemo obtenido, no consultado #$ nemo,
        paste0(
          ai$version |>
            sub(pat = ":..$", repl = "00") |>
            as.POSIXct(format = "%Y-%m-%dT%H:%M:%OS%z", tz="UTC") |>
            # tz = "America/Buenos_Aires"),
            format("%Y%m%dT%H%M", tz = tz),
          "_", ai$id, "_", adj$id))
      if(download.policy != "overwrite" && file.exists(path)) {
        message(gettextf("Archivo existe, omitiendo: %s", path))}
      else{
        message(gettextf("bajar %s en %s", url, path))
        httr::GET(
          url,
          httr::write_disk(path = path, overwrite = TRUE),
          httr::progress(), config = get.config)
      }
    }
  }

  data <- findDocumentosByNemo_resp(responses)
  unlink(tmpfile)
  on.exit(NULL)
  if (!NROW(data)) warning(gettextf("No se devolvieron datos para la consulta"))
  rbindlist(data)
}

#' Descarga adjuntos por nemo y docId
#'
#' @param nemo
#' @param docId
#' @param attachmentId
#' @param version
#' @param folder
#' @param file_pattern
#' @param overwrite
#'
#' @return list<httr::http_status>
#' @export
#'
#' @examples
findAttachmentByNemoId <- function(
    nemo, docId, attachmentId, version = NULL,
    folder = "~\\..\\OneDrive - Enel Spa\\cammesa\\downloads",
    file_pattern = function(version, docId, attachmentId) {
      paste0(version, "_", docId, "_", attachmentId)},
    overwrite = T)
{
  restUrl <- "https://api.cammesa.com/pub-svc/public/findAttachmentByNemoId"

  url <- mapply(
    nemo, docId, attachmentId, FUN = function(nemo, docId, attId) {
      httr::parse_url(restUrl)|>
        within.list(query <- list(
          nemo = nemo, docId = docId, attachmentId = attId)) |>
        httr::build_url()
    })

  version <- if (!is.null(version)) {
    if (inherits(version, "POSIXt")) {
      format(version, "%Y%m%dT%H%M", tz = "Etc/GMT+3")
    } else {
      if (!all(grepl(
        "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}([.]\\d+)?[+-]\\d{2}:\\d{2}$",
        version))) {
        stop(gettextf(
          "fecha debe proporcionarse como POSIXct o texto, por ejemplo %s",
          format(Sys.time(), format = "%Y-%m-%dT%H:%M:%OS%z", tz = "Etc/GMT+3") |>
            sub(pattern = "00$", replace = ":00")))
      } else {
        sub(":..$", "00", version) |>
          as.POSIXct(format = "%Y-%m-%dT%H:%M:%OS%z", tz = "UTC") |>
          format("%Y%m%dT%H%M", tz = "Etc/GMT+3") # tz = "America/Buenos_Aires"),
      }
    }
  }

  else "00000000T0000"

  #  dir.create(file.path(folder, nemo), showWarnings = F)
  #' TODO:
  #' nemo puede ser un grupo unificado como DTE_UNIF o PARTE_SEMANAL_UNIF
  #' decidir si va a la carpeta individual o unificada
  #' Prefiero la unificada pero hay que hacer la consulta para identificarlo.
  #' O BIEN : identificar los que son UNIF y probar con todos los nemos
  #' del grupo UNIF (preferible)
  #' usar un WARINING para avisar

  path <- file.path(
    folder, nemo, file_pattern(version, docId, attachmentId))

  mapply(url, path, FUN = function(url, path) {
    if (file.exists(path)) {
      message("Archivo existe, omitiendo: ", path)
      FALSE
    } else {
      if (!dir.exists(dirname(path)))
        dir.create(dirname(path))
      message("bajar ", url, " en ", path)
      if (file.exists(path) && overwrite == FALSE) {
        message(gettextf("Archivo %s existe, no se descarga", path))
        NULL
      } else {
        resp <- httr::GET(
          url,
          httr::write_disk(path = path),
          httr::progress())
        httr::warn_for_status(resp)
        httr::http_status(resp)
      }
    }
  })
}

#' Fecha último documento
#' Api REST: "https://api.cammesa.com/pub-svc/public/obtieneFechaUltimoDocumento"
#' @param nemo
#'
#' @return
#' @export
#'
#' @examples
obtieneFechaUltimoDocumento <- function(
    nemo = "DTE_UNIF") {
  apiUrl <- "https://api.cammesa.com/pub-svc/public/obtieneFechaUltimoDocumento"
  stopifnot(is.character(nemo))
  stopifnot(!anyDuplicated(nemo))
  do.call(rbind, lapply(nemo, \(i) {
    url <- httr::build_url(
      within.list(httr::parse_url(apiUrl), query <- list(nemo = i)))
    resp <- httr::GET(url)
    httr::warn_for_status(resp)
    data.frame(
      nemo = i,
      fecha = as.Date(httr::content(resp, simplifyVector = F)$fecha %||% NA))
  }))
}

# estrutura de la tabla de resultados
# query.struct <- structure(
#   list(id = character(0), fecha = character(0), nemo = character(0),
#        titulo = character(0), comentario = character(0), hora = character(0),
#        adjuntos_id = character(0), adjuntos_campo = character(0),
#        adjuntos_nombre = character(0),
#        version = structure(numeric(0), class = c("POSIXct", "POSIXt"), tzone = "UTC")),
#   row.names = c(NA, 0L),
#   class = c("data.table", "data.frame"))


#' Mantiene una tabla histórica de documentos consultados en un directorio local
#' @name querydt
#' @details
#' Esta función es provista para ayudar a mantener el histórico de consultas
#' a la API. Se ha comprobado que en ocasiones los archivos consultados
#' más viejos han dejado de estar públicos.
NULL

#' @name querydt
#' @param file
#'
#' @return
#' @export
#'
#' @examples
getQueryDT <- function(
    file = "~/../OneDrive - Enel Spa/cammesa/Downloads/query.csv") {
  dt <- fread(file = file)
  dt[, setattr(version, "tzone", "Etc/GMT+3")]
  dt[, comentario := fcoalesce(as.character(comentario), "")]
  dt[, titulo := fcoalesce(as.character(titulo), "")]

  tryCatch(error = \(e) stop(gettextf(
    "Error de control de tabla de documentos: %s", conditionMessage(e))),
    {
      # TODO: Más controles aquí
      stopifnot(!anyDuplicated(dt[, .(id, adjuntos_id)]))
    }
  )
  dt
}


#' @name querydt
#' @param x datos para actualizar la tabla, normalmente de
#' findDocumentosByNemo
#' @param file
#' @param force
#'
#' @return
#' @export
#'
#' @examples
updateQueryDT <- function(x,
    file = "~/../OneDrive - Enel Spa/cammesa/Downloads/query.csv",
    force = FALSE) {

  #' Consideraciones:
  #'  - las consultas con partes unificados (PARTE_SEMANAL_UNIF) no tienen
  #'  la misma "fecha" que las de los no unificados (ej PARTE_SEMANAL)
  #'  pasa lo mismo con la "hora" de PARTE_SEMANAL_COMP
  #'  Los ID's coinciden. No tengo idea si los archivos son idénticos.
  #'  - para datos viejos la query PARTE_SEMANAL_UNIF trae menos datos
  #'  que las de PARTE_SEMANAL individuales

  dt <- getQueryDT(file)

  x <- as.data.table(x)[,.SD, .SDcols = names(dt)]
  x[, comentario := fcoalesce(as.character(comentario), "")]
  x[, titulo := fcoalesce(as.character(titulo), "")]
  x[, version := as.POSIXct(version, tz = "UTC")]

  stopifnot(!anyDuplicated(x[, .(id, adjuntos_id)]))
  stopifnot(all.equal(names(x), names(dt)))

  x.dup  <- x[dt, on = c("id", "adjuntos_id"), .SD, nomatch = NULL]
  x.new  <- x[!dt, on = c("id", "adjuntos_id"), .SD]
  x.old <- dt[!x, on = c("id", "adjuntos_id"), .SD]

  if (!isTRUE(rs <- all.equal(
    x.dup, dt[x.dup, on = c("id", "adjuntos_id"), .SD]))) {
    msg <- gettextf(
      "Valores nuevos modifican datos en la tabla: %s",
      paste("\n - ", rs, collapse = ""))
    if (force) warning(msg) else stop(msg)
  }

  dt <- rbind(x.old, x.dup, x.new)

  fwrite(dt, file = file)

  message(
    gettextf("%d registro(s) nuevo(s) agregado a la base\n", nrow(x.dup)),
    gettextf("%d ya existía(n)\n",nrow(x.dup)),
    gettextf("%d registro(s) en total", nrow(dt)))

}
