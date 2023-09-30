#!/bin/bash

# This is a bash script with questions and answers for Data Engineering with Unix tools Data Fetching,
# Selection, Processing and Reporting at Business Analytics (A.U.E.B.) Part time 2022-2024
# In this link, you will find the form needed to be filled for the assignment: https://forms.gle/e4nmdiZNJMMsc8wQ6
# This data extraction was performed on 27/09/2023, with starting date 01/01/2023

curl -o oasa-datahist.txt.gz https://www.spinellis.gr/cgi-bin/oasa-history?id=x2899999 # <- replace this x2899999 with your student_id
gunzip oasa-datahist.txt.gz

# Q1: What is the data's field separator character? (1 point)
head -n 1 oasa-datahist.txt
awk -F',' '{print $1; exit}' oasa-datahist.txt
# A1: Comma is the data's field separator

# Q2: What happens when you try to load the data into a spreadsheet? (1 point)
# A2: It crashes with the error saying the file is larger than 1.048.576 rows to open it in excel for example.

# Q3: How many records are provided with the data? (1 point)
wc -l oasa-datahist.txt
# A3: 26.133.924 lines/records where found.

# Q4: What is the data acquisition time stamp of the last record in the stream you fetched? (1 point)
awk -F',' 'END{print $1}' oasa-datahist.txt
# A4: 2023-09-27T15:39:58 is the data acquisition time stamp of the last record in the stream.
    # Alternative code (takes more time though): cut -f1 -d, oasa-datahist.txt | tail -1

# Q5: How many different buses appear in the data? (1 point)
awk -F',' '!seen[$3]++ {count++} END {print count}' oasa-datahist.txt
# A5: 2012 different buses appear in the data.

# Q6: How many different routes are covered by the data? (1 point)
awk -F',' '!seen[$2]++ {count++} END {print count}' oasa-datahist.txt
# A6: 1065 different routes are covered by the data.

# Q7: How many dates are covered by the data? (2 points)
cut -d, -f4 oasa-datahist.txt | awk -F' ' '{count[$1 $2 $3]++} END {print length(count)}'
# A7: 270 different dates are covered by the data.

# Q8: Which route is associated with the most position reports? (2 points)
awk -F',' '{count[$3]++} END{max = 0; route = ""; for (i in count) { if (count[i] > max) { max = count[i]; route = i } } print route }' oasa-datahist.txt
# A8: 66249 is the route which associated with the most position reports.

# Q9: How many position reports appear in the data more than once? (2 points)
cut -d, -f2-6 oasa-datahist.txt | sort | uniq -d | wc -l
# A9: 579629 position reports appear in the data more than once.

# Q10: Which is the most frequent first two-digit sequence in numbers assigned to buses? (3 points)
awk -F',' '{count[substr($3, 1, 2)]++;} END {max = 0; seq = ""; for (s in count) { if (count[s] > max) { max = count[s]; seq = s } } print "Most frequent 2-digit sequence:", seq; print "Number of bus numbers starting with", seq ":", max; }' oasa-datahist.txt
# A10: 30 is the most frequent first two-digit sequence in numbers assigned to buses. 4.035.448 is the number of bus numbers starting with these digits.

# Q11: How many buses did not travel on this year's January 26th? (4 points)
comm -23 <(cut -d, -f3 oasa-datahist.txt | sort -u) <(grep "2023-01-26" oasa-datahist.txt | cut -d, -f3 | sort -u) | wc -l
# A11: 821 buses did not travel on this year's January 26th.

# Q12: On which date were the most buses on the road? (3 points)
awk '{print substr($0, 26, 17)}' oasa-datahist.txt | sort -u | cut -d, -f2 | uniq -c | sort -nr | head -1 | awk '{printf("     %s %04d-%02d-%02d\n", $1, $4, ($2 == "Jan" ? 1 : $2 == "Feb" ? 2 : $2 == "Mar" ? 3 : $2 == "Apr" ? 4 : $2 == "May" ? 5 : $2 == "Jun" ? 6 : $2 == "Jul" ? 7 : $2 == "Aug" ? 8 : $2 == "Sep" ? 9 : $2 == "Oct" ? 10 : $2 == "Nov" ? 11 : 12), $3)}'
# A12: 2023-04-12 was the date where the most buses (53) were on the road.

# Q13: Which route has been served by the highest number of different buses? (3 points)
cut -d, -f2,3 oasa-datahist.txt | sort -u | cut -d, -f1 | uniq -c | sort -nr | head -1
# A13: Route 2085 has been served by the highest number of different buses (387).

# Q14: On which hour of the day (e.g. 09) are there overall the most buses on the road? (3 points)
awk -F, '{split($4, date, / /); split(date[5], time, ":"); if(time[4] == "000PM" && time[1] != 12) time[1] += 12; else if(time[4] == "000AM" && time[1] == 12) time[1] = 0; hour = sprintf("%02d", time[1]); buses[hour][$3] = 1; } END { max_count = 0; max_hour = ""; for (hour in buses) { count = 0; for (bus in buses[hour]) { count++; } if (count > max_count) { max_count = count; max_hour = hour; } } print max_hour, max_count; }' oasa-datahist.txt
# A14: 12 p.m. is the hour of the day with overall the most buses on the road(2011 distinct buses in the whole dataset).

# Q15: On which hour of the day (e.g. 23) are there overall the fewest buses on the road? (3 points)
awk -F, '{split($4, date, / /); split(date[5], time, ":"); if(time[4] == "000PM" && time[1] != 12) time[1] += 12; else if(time[4] == "000AM" && time[1] == 12) time[1] = 0; hour = sprintf("%02d", time[1]); buses[hour][$3] = 1; } END { min_count = -1; min_hour = ""; for (hour in buses) { count = 0; for (bus in buses[hour]) { count++; } if (min_count == -1 || count < min_count) { min_count = count; min_hour = hour; } } print min_hour, min_count; }' oasa-datahist.txt
# A15: 3 p.m. is the hour of the day with overall the fewest buses on the road(825 distinct buses in the whole dataset).

# Q16: For which weekday (e.g. Wednesday) does your data set contain the most records? (5 bonus points)
awk -F, '{
    split($4, date, / /); 
    time = mktime(date[5] " " date[3] " " date[6] " " date[4] " " date[2] " " date[1]);
    weekday = strftime("%A", time);
    count[weekday]++;
} 
END {
    max_count = -1;
    max_weekday = "";
    for (weekday in count) {
        if (count[weekday] > max_count) {
            max_count = count[weekday];
            max_weekday = weekday;
        }
    }
    print max_weekday, max_count;
}' oasa-datahist.txt
# A16: Thursday is the day with most records(26.133.925).

# Q17: What are the bounding box geographic coordinates of the area served by the buses? (3 points)
awk -F',' 'BEGIN { min_lat = 90.0; max_lat = -90.0; min_lon = 180.0; max_lon = -180.0; } { lat = $NF; lon = $(NF-1); if (lat < min_lat) min_lat = lat; if (lat > max_lat) max_lat = lat; if (lon < min_lon) min_lon = lon; if (lon > max_lon) max_lon = lon; } END { print "Bounding box'\''s most northern latitude (degrees north): " max_lat; print "Bounding box'\''s most southern latitude (degrees north): " min_lat; print "Bounding box'\''s most western longitude (degrees east): " min_lon; print "Bounding box'\''s most eastern longitude (degrees east): " max_lon; }' oasa-datahist.txt
# A17: Bounding box's most northern latitude (degrees north): 24.0215410
    # Bounding box's most southern latitude (degrees north): 23.3200810
    # Bounding box's most western longitude (degrees east): 37.7172750
    # Bounding box's most eastern longitude (degrees east): 38.2233650

# Q18: Which bus has appeared closest to your favorite location? (4 points)
awk -F, -v lat=38.0340735267653 -v lon=23.736928865564032 '{
    R=6371;
    dLat=(lat-$5)*3.14159/180;
    dLon=(lon-$6)*3.14159/180;
    a=sin(dLat/2)*sin(dLat/2)+cos(lat*3.14159/180)*cos($5*3.14159/180)*sin(dLon/2)*sin(dLon/2);
    c=2*atan2(sqrt(a),sqrt(1-a));
    d=R*c;
    if(NR==1 || d<dist){
        dist=d;
        bus=$3;
        latBus=$5;
        lonBus=$6
    }
}
END{
    printf "Closest Bus: %s\n", bus;
    printf "Latitude: %s\n", latBus;
    printf "Longitude: %s\n", lonBus;
    printf "Distance (km): %.2f\n", dist;
}' oasa-datahist.txt
# A18: My favorite place is Old Dog for brunch on weekends, a café located in Nea Filadelphia, Athens, Greece.
    # The latitude and the longitude of this café are (38.0340735267653,23.736928865564032).
    # Closest Bus: 66274
    # Latitude: 38.0336860
    # Longitude: 23.7373520
    # Distance (km): 0.06 or 60 meters

# Q19: How many position reports have been sent by the chosen bus? (1 points)
closest_bus=$(awk -F',' -v lat=38.0340735267653 -v lon=23.736928865564032 '{
    R=6371;
    dLat=(lat-$5)*3.14159/180;
    dLon=(lon-$6)*3.14159/180;
    a=sin(dLat/2)*sin(dLat/2)+cos(lat*3.14159/180)*cos($5*3.14159/180)*sin(dLon/2)*sin(dLon/2);
    c=2*atan2(sqrt(a),sqrt(1-a));
    d=R*c;
    if(NR==1 || d<dist){
        dist=d;
        bus=$3;
        latBus=$5;
        lonBus=$6
    }
}
END{
    print bus;
}' oasa-datahist.txt)
awk -F',' -v bus="$closest_bus" '$3==bus {print $3 "," $4}' oasa-datahist.txt | sort -u | wc -l
# A19: 17043 position reports have been sent by the chosen bus (66274).

# Q20: What was the chosen bus's last position in the obtained data stream? (2 points)
awk -F',' -v bus="$closest_bus" '($3==bus) {last_lat=$5; last_lon=$6} END {printf "Last Latitude: %s\nLast Longitude: %s\n", last_lat, last_lon}' oasa-datahist.txt
# A20: Last Latitude: 37.9937620
    # Last Longitude: 23.7771350

# Q21: On which date has the chosen bus given the most position reports? (3 points)
awk -F, -v bus="$closest_bus" '{
    if ($3 == bus) {
        split($4, date, / /);
        date_str = date[3] "/" date[1] "/" date[4];
        date_count[date_str]++;
        if (date_count[date_str] > max_count) {
            max_count = date_count[date_str];
            max_date = date_str;
        }
    }
}
END {
    print max_count, max_date;
}' oasa-datahist.txt
# A21: On 4/Sep/2023 the chosen bus (66274) given the most position reports (180).

# Q22: On how many routes has the chosen bus traveled? (2 points)
awk -F, -v bus="$closest_bus" '{if ($3 == bus) routes[$2]++} END {print length(routes)}' oasa-datahist.txt
# A22: On 56 different routes the chosen bus (66274) has traveled.

# Q23: How many buses have shared at least one route with the chosen bus? (4 points)
awk -F, -v bus="$closest_bus" '$3==bus {routes[$2]=1} $3!=bus && $2 in routes {buses[$3]=1} END {for (b in buses) {print b}}' oasa-datahist.txt | sort -u | wc -l
# A23: 666 buses have shared at least one route with the chosen bus (66274).