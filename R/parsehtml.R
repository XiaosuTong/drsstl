# # ' @export
#
# parsehtml <- function(jobid, html, type="map") {
#
#   theurl <- file.path("http://wsc-adm.rcac.purdue.edu:19888/jobhistory/jobcounters", jobid)
#
#   a <- html_nodes(read_html(theurl), "tr")
#   L <- length(a)
#
#   if (type == "map") {
#
#     for (i in 1:L) {
#       r <- html_nodes(a[i], "td")
#       target <- as.character(html_attrs(r)[1])
#       if( target == "SPILLED_RECORDS") {
#         spill <- as.numeric(strsplit(html_text(r[2]), "[ \n]+")[[1]][2])
#       }
#       if( target == "MAP_OUTPUT_RECORDS") {
#         output <- as.numeric(strsplit(html_text(r[2]), "[ \n]+")[[1]][2])
#       }
#     }
#     return(spill/output)
#
#   } else {
#
#     for (i in 1:L) {
#       r <- html_nodes(a[i], "td")
#       target <- as.character(html_attrs(r)[1])
#       if( target == "SPILLED_RECORDS") {
#         spill <- as.numeric(strsplit(html_text(r[3]), "[ \n]+")[[1]][2])
#       }
#       if( target == "REDUCE_INPUT_RECORDS") {
#         output <- as.numeric(strsplit(html_text(r[3]), "[ \n]+")[[1]][2])
#       }
#     }
#     return(spill/output)
#
#   }
# }
