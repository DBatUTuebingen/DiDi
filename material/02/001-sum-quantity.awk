#!/usr/bin/env gawk -f

BEGIN  { FS = "|"
         sum = 0 }
       { sum = sum + $5 }
END    { print sum }
