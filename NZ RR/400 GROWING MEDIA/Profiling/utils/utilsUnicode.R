if(F)
{
  # Proof that the data is not encoded right in XXINV, but Snowflake does support UTF-8
  
  # This will require token authentication: FIXME
  con <- dbConnect(odbc::odbc(), "snowflake", encoding = "utf-8")
  
  sql = "select item_number, dw_strt_ts, attr_name, attr_value
         from bdwprd_srci.ebs_xxinv.xxinv_master_item_attr_all 
         where item_number = 432328  
         order by attr_name, dw_strt_ts"
  setDT(dbGetQuery(con, sql))
  
  sql = "select '×' as one_byte, '’' as two_byte, '😋' as three_byte"
  setDT(dbGetQuery(con, sql))
  
  
  
  #The first 128 code points (ASCII) need one byte. The next 1,920 code points need two bytes to encode,
  charToRaw("×")  #Later: \u00D7 = \xc3\x97 = "×"
  #[1] c3 97
  
  charToRaw('’')  #print('\xe2\x80\x99')
  #[1] e2 80 99
  
  charToRaw('😋')
  #[1] f0 9f 98 8b
}





UNICODE_PATTERN = '[^\x01-\x7F]'   # i.e. not ASCII
UNICODE_SUSPICIOUS_PATTERN = '(Â|â|Ã|á|Ë|à|ä)'

# (character test). technically non-ascii/non-null.  ASCII character values from 1 to 127
contains_unicode = function(s) { str_detect(s, UNICODE_PATTERN) }

# it seems these lists are equivalent, though the Â characters are two-byte:
#assert_that(identical(grepl(UNICODE_SUSPICIOUS_PATTERN, dt$DisplayName), contains_unicode(dt$DisplayName)))

# return unicode codepoints in string, remove empty string
unicode_extract = function(v) 
{ str_subset(str_sort(str_unique(str_extract_all(v, UNICODE_PATTERN, simplify = T))), '.+') }







#-----------------------------------------------------------------------------------------------------------------
# 4-byte UTF-8 (presumably) gets expanded to 8 bytes.  Yet to observe.


#-----------------------------------------------------------------------------------------------------------------
# 3-byte UTF-8 gets expanded to 6-bytes.  "FrogTape 36mm x 55m Blue Pro Grade Painterâ\u0080\u0099s Tape - 4 Pack"
# charToRaw('â\u0080\u0099')
#[1] c3 a2 c2 80 c2 99
fix_utf8_3byte = function(s)
{
  if (is.na(s)) return(s)
  if (is.null(s)) return(s)
  if (s == '') return(s)
  if (!contains_unicode(s)) return(s)
  
  locs = grepRaw('\xc3[\x80-\xFF]\xc2[\x80-\xFF]\xc2[\x80-\xFF]', s, all = T)
  if (length(locs) == 0) return(s)
  
  s_raw = charToRaw(s)
  s_len = length(s_raw)
  
  for (i in 1:length(locs))
  { 
    loc = locs[i]
    
    first_byte = NULL
    if (s_raw[loc+1] == charToRaw('\x82')) { first_byte = charToRaw('\xc2') }
    if (s_raw[loc+1] == charToRaw('\xa1')) { first_byte = charToRaw('\xe1') }
    if (s_raw[loc+1] == charToRaw('\xa2')) { first_byte = charToRaw('\xe2') }
    if (s_raw[loc+1] == charToRaw('\xa3')) { first_byte = charToRaw('\xe3') }
    if (s_raw[loc+1] == charToRaw('\xaf')) { first_byte = charToRaw('\xef') }
    if (is.null(first_byte)) { message('Saw new type of unicode character (3-byte)!'); browser() }
    
    #print(s)
    # charToRaw('Â¿¿')
    # [1] c3 82 c2 bf c2 bf
    # charToRaw('【')
    # [1] e3 80 90
    
    # First byte must alway starts with an 'E'.
    utf8_bytes = c(first_byte, s_raw[loc+3], s_raw[loc+5])
    s_raw = c(s_raw[-c(loc:s_len)], utf8_bytes, s_raw[-c(1:(loc+5))])  # replace the 6 bytes with the 3
    locs = locs - 3
  }
  
  return(rawToChar(s_raw))  
}
# fix_utf8_3byte('â\u0080\u0099')



#-----------------------------------------------------------------------------------------------------------------
# 2-byte UTF-8 gets expanded to 4 bytes. "Heat BeadsÂ® BBQ Chimney" 
# charToRaw('Â®')
# [1] c3 82 c2 ae
# charToRaw("Ã\u0097")  # "K-Rain 19mm Barbed Tee Ã\u0097 15mm BSP Male - 10 Pack"
# [1] c3 83 c2 97
# charToRaw('Ë\u009a')
# [1] c3 8b c2 9a
#  charToRaw('Î©')
# [1] c3 8e c2 a9

fix_utf8_2byte = function(s)
{
  return(s)   #WHY?? - must have come about after switch from Excel to Snowlake
  
  if (is.na(s)) return(s)
  if (is.null(s)) return(s)
  if (s == '') return(s)
  if (!contains_unicode(s)) return(s)
  
  locs = grepRaw('\xc3[\x80-\xFF]\xc2[\x80-\xFF]', s, all = T)
  if (length(locs) == 0) return(s)
  
  s_raw = charToRaw(s)
  s_len = length(s_raw)
  
  for (i in 1:length(locs))
  { 
    loc = locs[i]
    
    first_byte = NULL
    
    if (s_raw[loc+1] == charToRaw('\x80')) { first_byte = charToRaw('\xc0') }
    if (s_raw[loc+1] == charToRaw('\x81')) { first_byte = charToRaw('\xc1') } 
    if (s_raw[loc+1] == charToRaw('\x82')) { first_byte = charToRaw('\xc2') } 
    if (s_raw[loc+1] == charToRaw('\x83')) { first_byte = charToRaw('\xc3') }
    if (s_raw[loc+1] == charToRaw('\x84')) { first_byte = charToRaw('\xc4') }
    if (s_raw[loc+1] == charToRaw('\x85')) { first_byte = charToRaw('\xc5') }
    if (s_raw[loc+1] == charToRaw('\x86')) { first_byte = charToRaw('\xc6') } 
    if (s_raw[loc+1] == charToRaw('\x87')) { first_byte = charToRaw('\xc7') } 
    if (s_raw[loc+1] == charToRaw('\x88')) { first_byte = charToRaw('\xc8') }
    if (s_raw[loc+1] == charToRaw('\x89')) { first_byte = charToRaw('\xc9') }
    if (s_raw[loc+1] == charToRaw('\x8a')) { first_byte = charToRaw('\xca') }
    if (s_raw[loc+1] == charToRaw('\x8b')) { first_byte = charToRaw('\xcb') }
    if (s_raw[loc+1] == charToRaw('\x8c')) { first_byte = charToRaw('\xcc') }
    if (s_raw[loc+1] == charToRaw('\x8d')) { first_byte = charToRaw('\xcd') }
    if (s_raw[loc+1] == charToRaw('\x8e')) { first_byte = charToRaw('\xce') }
    if (s_raw[loc+1] == charToRaw('\x8f')) { first_byte = charToRaw('\xcf') }
    
    if (s_raw[loc+1] == charToRaw('\x90')) { first_byte = charToRaw('\xd0') }
    if (s_raw[loc+1] == charToRaw('\x91')) { first_byte = charToRaw('\xd1') }
    if (s_raw[loc+1] == charToRaw('\x92')) { first_byte = charToRaw('\xd2') }
    if (s_raw[loc+1] == charToRaw('\x93')) { first_byte = charToRaw('\xd3') }
    if (s_raw[loc+1] == charToRaw('\x94')) { first_byte = charToRaw('\xd4') }
    if (s_raw[loc+1] == charToRaw('\x95')) { first_byte = charToRaw('\xd5') }
    if (s_raw[loc+1] == charToRaw('\x96')) { first_byte = charToRaw('\xd6') }
    if (s_raw[loc+1] == charToRaw('\x97')) { first_byte = charToRaw('\xd7') }
    if (s_raw[loc+1] == charToRaw('\x98')) { first_byte = charToRaw('\xd8') }
    if (s_raw[loc+1] == charToRaw('\x99')) { first_byte = charToRaw('\xd9') }
    if (s_raw[loc+1] == charToRaw('\x9a')) { first_byte = charToRaw('\xda') }
    if (s_raw[loc+1] == charToRaw('\x9b')) { first_byte = charToRaw('\xdb') }
    if (s_raw[loc+1] == charToRaw('\x9c')) { first_byte = charToRaw('\xdc') }
    if (s_raw[loc+1] == charToRaw('\x9d')) { first_byte = charToRaw('\xdd') }
    if (s_raw[loc+1] == charToRaw('\x9e')) { first_byte = charToRaw('\xde') }
    if (s_raw[loc+1] == charToRaw('\x9f')) { first_byte = charToRaw('\xdf') }
    if (is.null(first_byte)) { message('Saw new type of unicode character (2-byte)!'); browser() }        
    
    # print(s)
    # Browse[3]> charToRaw('Å\u008d')
    # [1] c3 85 c2 8d
    # Browse[3]> charToRaw('Х')     # find actual unicode by googling to the website page
    # [1] d0 a5
    
    
    utf8_bytes = c(first_byte, s_raw[loc+3])
    s_raw = c(s_raw[-c(loc:s_len)], utf8_bytes, s_raw[-c(1:(loc+3))])  # replace the 4 bytes with the 2
    locs = locs - 2
  }
  
  return(rawToChar(s_raw))  
}
# fix_utf8_2byte('Â®')
# fix_utf8_2byte("Ã\u0097")




#-----------------------------------------------------------------------------------------------------------------
# 1-byte characters in UTF-8 are the same as ASCII.  Nothing happens to them.









#-----------------------------------------------------------------------------------------------------------------
# Every byte within a UTF-8 gets expanded to '\u00xx' (6 bytes)
fix_utf8_exploded = function(s)
{
  if (is.na(s)) return(s)
  if (is.null(s)) return(s)
  if (s == '') return(s)
  
  locs = grepRaw('\\\\u00[0-9A-F][0-9A-F]', s, all = T)
  if (length(locs) == 0) return(s)
  
  s_raw = charToRaw(s)
  s_len = length(s_raw)
  
  for (i in 1:length(locs))
  { 
    loc = locs[i]
    
    first_byte = NULL
    
    text = stri_c('0x', rawToChar(c(s_raw[loc+4], s_raw[loc+5])))
    #browser()
    
    utf8_byte = as.raw(eval(parse(text=text)))
    s_raw = c(s_raw[-c(loc:s_len)], utf8_byte, s_raw[-c(1:(loc+5))])  # replace the 6 bytes with the 1
    locs = locs - 5
  }
  
  return(rawToChar(s_raw))  
}    







#-----------------------------------------------------------------------------------------------------------------
# Finishing - sometimes the user has inputted a malformed unicode which is "valid" unicode direct into EBS,
# and it shows up in the website.  Other times it seems to be a left over 'Â' from somewhere

# dtwsc[DisplayName %like% 'Â']$DisplayName
# [1] "Zenith 25mm Stainless Steel Angle BracketÂ - 12 Pack"              
# [2] "Zenith 38mm Stainless Steel Angle BracketÂ - 12 Pack"              
# [3] "ZincalumeÂ® Flashing .55 x 150mm FZA0150"                          
# [4] "ZincalumeÂ® Flashing .55 x 200mm FZA0200"                          
# [5] "ZincalumeÂ® Flashing .55 x 250mm FZA0250"                          
# [6] "ZincalumeÂ® Flashing .55 x 300mm FZA0300"                          
# [7] "ZincalumeÂ® Flashing .55 x 50mm FZA0050"                           
# [8] "ZincalumeÂ® Flashing .55 x 100mm FZA0100"                          
# [9] "ProtectorAl 1010 mm H Black Aluminium Flanged 90Â° Balustrade Post"
# [10] "ProtectorAl 1010 mm H White Aluminium Flanged 90Â° Balustrade Post"

# sort(dtwsc[Description %like% '\u009d']$Description)
# [1] "Within this Sidchrome TorquePlus, all standard and deep hand sockets and ring spanners feature the TorquePlus fastening system.  This unique system allows greater contact with the flats of fasteners, providing greater torque to enable easier fastening.\r\nConventional sockets damage fasteners by concentrating forces on the corners. \r\nKit Includes:\r\n- 10 Metric 3/8\" drive sockets 10, 11, 12, 13, 14, 15, 16, 17, 18 & 19mm\r\n- 9 A/F 3/8\" drive sockets 3/8, 7/16, 1/2, 9/16, 5/8, 11/16, 3/4, 13/16 & 7/8\"\u009d\r\n- 1 Ratchet 3/8\"\u009d drive with new grip\r\n- 1 x 3/8\"\u009d drive extension 150mm\r\n- 1 x 3/8\"\u009d drive universal joint\r\n- 1 x Adjustable wrench 250mm\r\n- 10 Metric Ring and Open End Spanners 10, 11, 12, 13, 14, 15, 16, 17, 18, 19mm\r\n- 1 Inspection mirror\r\n- 1 Magnetic pick up tool\r\n- 18 Hex Keys - Metric and A/F\r\n"

fix_utf8_finishing = function(s)
{
  
  if (is.na(s)) return(s)
  if (is.null(s)) return(s)
  if (s == '') return(s)
  if (!contains_unicode(s)) return(s)
  
  
  # I don't know why these appear sometimes and not other times.  I suspect the way snowflake returns malformed unicode
  # is non-deterministic at current.  Tried specifying the encoding parameter to dbConect to 'latin1' and 'UTF-8', no dice.
  
  # if you plug in the code into the following site, and scroll down to the 'Java Data' section, you seem to get what 
  # was intended:
  # https://www.fileformat.info/info/unicode/char/008c/index.htm
  
  
  
  
  
  
  
  
  
  ###### THESE HAVE BROKEN IN LATEST VERSION OF R
  #https://stackoverflow.com/questions/76680882/unable-to-translate-to-a-wide-string
  #https://blog.r-project.org/2022/06/27/why-to-avoid-%5Cx-in-regular-expressions/
  
  
  s = str_replace_all(s, fixed('\u0081'), '')  #control char
  s = str_replace_all(s, fixed('\u0082'), '‚') 
  s = str_replace_all(s, fixed('\u0083'), 'ƒ') 
  s = str_replace_all(s, fixed('\u0084'), '„') 
  s = str_replace_all(s, fixed('\u0085'), '…') 
  s = str_replace_all(s, fixed('\u0086'), '†') 
  s = str_replace_all(s, fixed('\u0089'), '‰') 
  s = str_replace_all(s, fixed('\u008a'), 'Š') 
  s = str_replace_all(s, fixed('\u008c'), 'Œ') 
  s = str_replace_all(s, fixed('\u008d'), '')  #control char
  # 
  s = str_replace_all(s, fixed('\u0092'), '’') 
  s = str_replace_all(s, fixed('\u0093'), '“') 
  s = str_replace_all(s, fixed('\u0094'), '”') 
  s = str_replace_all(s, fixed('\u0096'), '–') 
  s = str_replace_all(s, fixed('\u0097'), '—') 
  s = str_replace_all(s, fixed('\u0098'), '˜') 
  s = str_replace_all(s, fixed('\u0099'), '™') 
  s = str_replace_all(s, fixed('\u009a'), 'š') 
  s = str_replace_all(s, fixed('\u009c'), 'œ') 
  s = str_replace_all(s, fixed('\u009d'), '')  #control char
  
  # these are effectively 2+2 or 2+3 byte characters being converted to 2-byte characters
  s = str_replace_all(s, fixed('Â®'), '®')  
  s = str_replace_all(s, fixed('Ã‰'), 'É')
  s = str_replace_all(s, fixed('Ã©'), 'é')
  s = str_replace_all(s, fixed('Ã¨'), 'è')
  s = str_replace_all(s, fixed('Ã\xc2\xa0'), 'à')  #Marquee 1.6m DéjÃ  Vu Blue And Agret Beach Umbrella
  s = str_replace_all(s, fixed('Ã¡'), 'á')
  s = str_replace_all(s, fixed('Ã¤'), 'ä')
  s = str_replace_all(s, fixed('Ã¶'), 'ö')
  s = str_replace_all(s, fixed('Ã§'), 'ç')
  
  s = str_replace_all(s, fixed('Ã˜'), 'Ø')
  s = str_replace_all(s, fixed('Ã¸'), 'ø')
  s = str_replace_all(s, fixed('Ëš'), '°')
  
  s = str_replace_all(s, fixed('Ã—'), '×')
  s = str_replace_all(s, fixed('Ã‚'), '')  # nothing
  s = str_replace_all(s, fixed('Â'), '')  # nothing
  
  return(s)
}






fix_utf8 = function(v) 
{ 
  unlist(lapply(  
    unlist(lapply(  
      unlist(lapply(v, 
                    fix_utf8_3byte)),
      fix_utf8_2byte)),
    fix_utf8_finishing)) 
}

fix_utf8_KSP = function(v)   # KeySellingPoints has a different type of garbling!
{ 
  unlist(lapply(  
    unlist(lapply(v, 
                  fix_utf8_exploded)), 
    fix_utf8_finishing)) 
}


fix_double_quotes = function(v) { return(str_replace_all(v, '\"\"', '\"')) } # double quotes got doubled up somewhere 


# This converts esoteric open/close quote pairings to '...'
unicode_normalise_quotes = function(v)
{
  v = str_replace_all(v, '‘(.+)’', "'\\1'")
  v = str_replace_all(v, '“(.+)”', "'\\1'")
  
  v = str_replace_all(v, '‵(.+)′', "'\\1'")   # These are actually mathematical prime symbols
  v = str_replace_all(v, '‶(.+)″', "'\\1'")
  return(v)
}


# See examples below, this doesn't normalise everything. We use it mainly for handling accented latin characters
# after substituting other unicodes. To treat unicode that doesn't get subbed here, use config/subs_unicode.csv.
unicode_normalise_ascii_besteffort = function(v) 
{ 
  ret = stri_trans_general(v, 'latin-ascii')
  return(ret)  
}

# unicode_normalise_ascii_besteffort(c("üéâäàåçêëèïîì", toupper("üéâäàåçêëèïîì"), "­️ ‑–‘’“”′″˚°®×⁄⌀⁰½¼²³¾⅜⅝àÂäçéᴹºøØᵀ™µ"))
# [1] "ueaaaaceeeiii"                                           
# [2] "UEAAAACEEEIII"                                           
# [3] "-️ --''\"\"'\"˚°(R)*/⌀⁰ 1/2 1/4²³ 3/4 3/8 5/8aAaceᴹºoOᵀ™µ"