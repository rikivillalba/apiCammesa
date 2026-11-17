#API CAMMESA

#' @import data.table
#' @import backports
NULL

.query_dt_ptype <- data.table::data.table(
  id = character(0), fecha = character(0), nemo = character(0),
  titulo = character(0), comentario = character(0), hora = character(0),
  adjuntos_id = character(0), adjuntos_campo = character(0),
  adjuntos_nombre = character(0),
  version = as.POSIXct(numeric(0), tzone = "Etc/GMT+3")
)

# Funciones auxiliares
messagef <- function (fmt, ..., domain = NULL, appendLF = TRUE) {
  message(sprintf(fmt, ...), domain = domain, appendLF = appendLF)
}


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
catalogoPublicaciones <- function(vigentes = FALSE) {
  url <- if (isTRUE(vigentes))
    "https://api.cammesa.com/pub-svc/public/catalogoPublicacionesVigentes"
  else
    "https://api.cammesa.com/pub-svc/public/catalogoPublicaciones"
  message("Consultando ", url, "...")
  response <- try_get(url)
  httr::content(response, simplifyVector = T)
}

#' Descripción de la api de cammesa
#' "https://api.cammesa.com/pub-svc/v2/api-docs"
#' @return
#' @export
#'
#' @examples
api_docs <- function() {
  httr::content(
    httr::GET("https://api.cammesa.com/pub-svc/v2/api-docs"),
    simplifyVector = F
  )
}

try_get <- function(url, config = list(), ...) {
  nmax <- 3L
  ntry <- 1L
  repeat {
    tryCatch(
      curl_error = \(e) message(conditionMessage(e)),
      http_error = \(e) message(conditionMessage(e)),
      return(httr::stop_for_status(httr::GET(url, config = config, ...)))
    )
    if (ntry == nmax) stop("Demasiados intentos fallidos")
    ntry <- ntry + 1L
    message("intento número ", ntry, ":")
  }
}

#' @author author
.onLoad <- function(libname, pkgname) {
  backports::import(pkgname, "R_user_dir")
  cache = tools::R_user_dir("apiCammesa", "cache")
  data = tools::R_user_dir("apiCammesa", "data")
  options(
    apiCammesa.download_folder = cache,
    apiCammesa.download_database = file.path(data, "querydata.csv")
  )
}

#' Buscar y opcionalmente descargar documentos de CAMMESA por fecha y NEMO
#'
#' @param nemo nemo de clase de documento de la API de cammesa
#' @param fechadesde fecha de consulta desde, fechas desde
#' "2010-06-01T03:00:00.000Z" fueron probadas
#' @param fechahasta fecha de consulta hasta
#' @param folder ruta base de descarga (los archivos se descargan en una
#' @param pattern
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
  fechadesde = as.POSIXct(trunc(Sys.time() - 30L * 86400L, "month"), tz = tz),
  fechahasta = as.POSIXct(Sys.time(), tz = tz),
  tz = "America/Buenos_Aires",
  folder = getOption("apiCammesa.download_folder"),
  pattern = ".*",
  download.policy = c("none", "missing.only", "overwrite"),
  queryByMonth = FALSE,
  responses_from_backup = NULL
) {
  stopifnot(is.character(nemo))
  stopifnot(is.atomic(fechadesde) && length(fechadesde) == 1L)
  stopifnot(is.atomic(fechahasta) && length(fechahasta) == 1L)
  download.policy <- match.arg(download.policy)

  fd <- parseFecha(fechadesde, tz)
  fh <- parseFecha(fechahasta, tz)
  if (queryByMonth) {
    fechas <- splitYM(seq.POSIXt(fd, fh, "days"))
    fd <- as.POSIXct(vapply(fechas, head, 0, 1), "UTC")
    fh <- as.POSIXct(vapply(fechas, tail, 0, 1), "UTC")
  }

  tabla <- merge(
    data.frame(
      fechadesde = format(fd, format = "%Y-%m-%dT%H:%M:%OS3Z"),
      fechahasta = format(fh, format = "%Y-%m-%dT%H:%M:%OS3Z")
    ),
    data.frame(nemo = nemo)
  )

  stopifnot(nrow(tabla) > 0L)
  endpoint <- "https://api.cammesa.com/pub-svc/public/findDocumentosByNemoRango"

  if (!is.null(responses_from_backup)) {
    responses <- responses_from_backup
  } else {
    responses <- lapply(1:nrow(tabla), function(i) {
      fd <- tabla[i, "fechadesde"]
      fh <- tabla[i, "fechahasta"]
      messagef("Consultando desde %s hasta %s", fd, fh)
      query = list(fechadesde = fd, fechahasta = fh, nemo = tabla[i, "nemo"])
      url <- httr::modify_url(endpoint, query = query)
      message("request ", utils::URLdecode(url))
      try_get(url)
    })
    tmpfile <- tempfile("findDocumentosByNemo", fileext = "rds")
    saveRDS(responses, tmpfile)
    on.exit(message(gettextf("datos descargados guardados en %s", tmpfile)))
  }

  content <- responses |>
    lapply(httr::content, type = "application/json", simplifyVector = FALSE)
  stopifnot(length(content) == nrow(tabla))

  ok <- !vapply(content, hasName, logical(1), "status")
  for(i in content[!ok]) {
    message(gettextf("AVISO: %s (%s)\n", i$message, i$error_code))
    next
  }

  content <- Map(
    \(ctnt, cols) lapply(ctnt, replace, paste0("tbl_", names(cols)), cols),
    ctnt = content[ok], cols = split(tabla, 1:nrow(tabla))[ok]
  )

  #  data <- findDocumentosByNemo_resp(content[valid])
#  valid <- vapply(content, \(i) is.null(i$error_code), FALSE)
#  data <- unlist(lapply(content[valid], make_doc_table), FALSE, FALSE)
  if (length(content)) {
    if (download.policy %in% c("missing.only", "overwrite")) {
      download_content(
        content, pattern, folder, tz, download.policy == "overwrite"
      )
    }
    data <- make_doc_table(content) #   , pattern
  } else {
    warning("No se generó tabla de descargas")
    data <- NULL
  }
  unlink(tmpfile)
  on.exit(NULL)
  if (!NROW(data)) {
    warning(gettextf("No se devolvieron datos para la consulta"))
  }
  data
}

download_content <- function(content, pattern, folder, tz, overwrite) {
  endpoint <- "https://api.cammesa.com/pub-svc/public/findAttachmentByNemoId"
  download_error_handler <- function(e) {
    messagef("AVISO: No se pudo descargar: %s", conditionMessage(e))
  }
  for (ctnt in content) {
    for (ai in ctnt) {
      for (adj in ai$adjuntos) {
        if (!grepl(pattern, adj$id)) next
        cat("ai=", ai$id, " adj=", adj$id, "\n")
        if (ai$tbl_nemo != ai$nemo) {
          # usa nemo obtenido, no consultado
          tn <- paste(ai$tbl_nemo, ai$nemo, sep = "->")
          message("NOTA: Carpeta de destino ", tn)
        }
        dir.create(file.path(folder, ai$nemo), showWarnings = FALSE)
        ai$version <- sub(":..$", "00", ai$version)  # 03:00 -> 0300
        ver <- as.POSIXct(ai$version, "%Y-%m-%dT%H:%M:%OS%z", tz = "UTC")
        fversion <- format(ver, "%Y%m%dT%H%M", tz = tz)
        fname <- paste0(fversion, "_", ai$id, "_", adj$id)
        path <- file.path(folder, ai$nemo, fname)
        if (!overwrite && file.exists(path)) {
          messagef("Archivo existe, omitiendo: %s", path)
          next
        }
        query <- list(nemo = ai$nemo, docId = ai$id, attachmentId = adj$id)
        url <- httr::modify_url(endpoint, query = query)
        message(gettextf("bajar %s en %s", url, path))
        tryCatch(error = download_error_handler, try_get(
          url, httr::progress(), httr::write_disk(path, overwrite = TRUE)
        ))
      }
    }
  }
  invisible()
}

make_doc_table <- function(content) {
  adj_default <- list(
    id = NA_character_,
    campo = NA_character_,
    nombre = NA_character_
  )
  make_rows <- function (i) {
    if (!length(i[["adjuntos"]])) i[["adjuntos"]] <- list(adj_default)
    lapply(i[["adjuntos"]], function(adj) {
      names(adj) <- paste("adjuntos", names(adj), sep = "_")
      k <- append(i, adj, after = match("adjuntos", names(i)))
      k[names(k) != "adjuntos"]
    })
  }
  content <- content |>
    unlist(recursive = FALSE) |>
    Filter(f = \(i) !hasName(i, "error_code")) |>
    lapply(make_rows) |>
    unlist(recursive = FALSE) |>
    rbindlist()
  content[, comentario := fcoalesce(as.character(comentario), "")]
  content[, titulo := fcoalesce(as.character(titulo), "")]
  content[, grep("^tbl_", names(.SD), value = TRUE) := NULL]
  content[, version := as.POSIXct(
    sub(":..$","00", version), format = "%Y-%m-%dT%H:%M:%OS%z", tz = "UTC")]
  content[]
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
    folder = getOption("apiCammesa.download_folder"),
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
  } else {
    "00000000T0000"
  }

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
    file = getOption("apiCammesa.download_database")
) {
  # OJO con na.strings, por defecto los adjuntos_id vacíos son NA en el request
  # fwrite lo guarda como <,,> por lo que na.strings = "" evita duplicados.
  dt <- vctrs::vec_cast(fread(file, na.strings = ""), .query_dt_ptype)
  #dt[, setattr(version, "tzone", "Etc/GMT+3")]
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
#'
# Consideraciones:
#  - las consultas con partes unificados (PARTE_SEMANAL_UNIF) no tienen
#  la misma "fecha" que las de los no unificados (ej PARTE_SEMANAL)
#  pasa lo mismo con la "hora" de PARTE_SEMANAL_COMP
#  Los ID's coinciden. No tengo idea si los archivos son idénticos.
#  - para datos viejos la query PARTE_SEMANAL_UNIF trae menos datos
#  que las de PARTE_SEMANAL individuales
updateQueryDT <- function(x,
  file = getOption("apiCammesa.download_database"),
  force = FALSE
) {
  dt <- getQueryDT(file)

  #TODO: sacar vctrs
  x <- vctrs::vec_cast(as.data.table(x), .query_dt_ptype)
  x[, comentario := fcoalesce(as.character(comentario), "")]
  x[, titulo := fcoalesce(as.character(titulo), "")]
  #x[, version := as.POSIXct(version, tz = "Etc/GMT+3")]
  stopifnot(!anyDuplicated(x[, .(id, adjuntos_id)]))
  stopifnot(all.equal(names(x), names(dt)))

  x.dup <- x[dt, on = c("id", "adjuntos_id"), .SD, nomatch = NULL]
  x.new <- x[!dt, on = c("id", "adjuntos_id"), .SD]
  x.old <- dt[!x, on = c("id", "adjuntos_id"), .SD]

  if (!isTRUE(rs <- all.equal(
    x.dup, dt[x.dup, on = c("id", "adjuntos_id"), .SD]))) {
    msg <- gettextf(
      "Valores nuevos modifican datos en la tabla: %s",
      paste("\n - ", rs, collapse = ""))
    if (force) warning(msg) else stop(msg)
  }
  dt <- rbind(x.old, x.dup, x.new)
  data.table::fwrite(dt, file = file)
  message(
    gettextf("%d registro(s) nuevo(s) agregado a la base\n", nrow(x.dup)),
    gettextf("%d ya existía(n)\n",nrow(x.dup)),
    gettextf("%d registro(s) en total", nrow(dt))
  )
}
