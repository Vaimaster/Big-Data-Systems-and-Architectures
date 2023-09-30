#----------------------------------Assignment in Big Data Systems-------------------------------------#
#-------------------------------------------- REDIS/TASK 1-------------------------------------------#

# Load the library
if (!requireNamespace("redux", quietly = TRUE)) {
  install.packages("redux")}
library("redux")

# Local Connection
redis_con <- redux::hiredis(
  redux::redis_config(
    host = "127.0.0.1", 
    port = "6379"))

# Load data sets
#setwd() # <-- set your working directory here
listings <- read.csv("modified_listings.csv", header = TRUE, sep=",", stringsAsFactors = FALSE)
emails <- read.csv("emails_sent.csv", header = TRUE, sep=",", stringsAsFactors = FALSE)

# Question 1.1: Users modified their listing on January

for(i in 1:nrow(listings))
{
  if ((listings$ModifiedListing[i] ==1) & (listings$MonthID[i]==1)) 
  {
    redis_con$SETBIT("ModificationsJanuary",listings$UserID[i],"1")
  }
}

q1_modified_jan_count <- redis_con$BITCOUNT("ModificationsJanuary")
print(paste('Users Modified in January:',q1_modified_jan_count),quote = FALSE)

# Question 1.2: Users not modified their listing on January

invisible(redis_con$BITOP("NOT","NoModificationsJanuary","ModificationsJanuary"))
q2_not_modified_count <- redis_con$BITCOUNT("NoModificationsJanuary")
print(paste('Users not Modified in January:',q2_not_modified_count),quote = FALSE)
cat('Redis stores binary strings using whole bytes, so even if only a few bits are needed to store a value, Redis will use a whole byte.\nWith 19999 users, Redis would need 19999 bits, which is equivalent to 2499,875 bytes.\nSince Redis uses whole bytes, it would round up to 2500 bytes to store the data.')

# Question 1.3: Users received at least one e-mail per month

for(i in 1:nrow(emails))
{
  if (emails$MonthID[i]==1)
  {
    redis_con$SETBIT("EmailsJanuary",emails$UserID[i],"1")
  }
  else if(emails$MonthID[i]==2)
  {
    redis_con$SETBIT("EmailsFebruary",emails$UserID[i],"1")
  }
  else if (emails$MonthID[i]==3)
  {
    redis_con$SETBIT("EmailsMarch",emails$UserID[i],"1")
  }
}

invisible(redis_con$BITCOUNT("EmailsJanuary"))
invisible(redis_con$BITCOUNT("EmailsFebruary"))
invisible(redis_con$BITCOUNT("EmailsMarch"))
invisible(redis_con$BITOP("AND","ReceivedAtLeast1EmailPerMonth",c("EmailsJanuary","EmailsFebruary" , "EmailsMarch")))

q3_users_with_at_least_one_email <- redis_con$BITCOUNT("ReceivedAtLeast1EmailPerMonth")
print(paste('Users who received at least one mail per month:',q3_users_with_at_least_one_email), quote = FALSE)

# Question 1.4: Users received e-mail only on January and March

invisible(redis_con$BITOP("AND","ReceivedAtLeast1EmailJanMar",c("EmailsJanuary","EmailsMarch")))
invisible(redis_con$BITCOUNT("ReceivedAtLeast1EmailJanMar"))

invisible(redis_con$BITOP("NOT", "NoEmailsFebruary", "EmailsFebruary"))
invisible(redis_con$BITCOUNT("NoEmailsFebruary"))

invisible(redis_con$BITOP("AND","ReceivedJanMarButNoFeb",c("ReceivedAtLeast1EmailJanMar","NoEmailsFebruary")))

q4_users_with_at_received_email_JanMar_but_not_Feb <- redis_con$BITCOUNT("ReceivedJanMarButNoFeb")
print(paste('Users who received at least one mail in January and March but not in February:',q4_users_with_at_received_email_JanMar_but_not_Feb), quote = FALSE)

# Question 1.5: Users received e-mail on January which was not opened but their listing is updated

for(i in 1:nrow(emails))
{
  if ((emails$MonthID[i]==1) & (emails$EmailOpened[i]==0))
  {
    redis_con$SETBIT("NotOpenJanuary",emails$UserID[i],"1")
  }
}

invisible(redis_con$BITOP("AND","ReceivedNoOpenButModifiedJanuary",c("ModificationsJanuary","NotOpenJanuary")))

q5_users_who_received_didnt_open_and_modified_jan <- redis_con$BITCOUNT("ReceivedNoOpenButModifiedJanuary")
print(paste('Users who received at least one mail in January, didn\'t open it and they updated their listing:',q5_users_who_received_didnt_open_and_modified_jan), quote = FALSE)
# Question 1.6: Users received e-mail which was not opened but their listing is updated (on January or February or March)

for(i in 1:nrow(listings))
{
  if ((listings$ModifiedListing[i] ==1) & (listings$MonthID[i]==2)) 
  {
    redis_con$SETBIT("ModificationsFebruary",listings$UserID[i],"1")
    
  }
  else if ((listings$ModifiedListing[i] ==1) & (listings$MonthID[i]==3)) {
    redis_con$SETBIT("ModificationsMarch",listings$UserID[i],"1")
  }
}

for(i in 1:nrow(emails))
{
  if ((emails$MonthID[i]==2) & (emails$EmailOpened[i]==0))
  {
    redis_con$SETBIT("NotOpenFebruary",emails$UserID[i],"1")
  }
  else if ((emails$MonthID[i]==3) & (emails$EmailOpened[i]==0))
  {
    redis_con$SETBIT("NotOpenMarch",emails$UserID[i],"1")
  }
}

invisible(redis_con$BITOP("AND","ReceivedNoOpenButModifiedFebruary",c("ModificationsFebruary","NotOpenFebruary")))
invisible(redis_con$BITOP("AND","ReceivedNoOpenButModifiedMarch",c("ModificationsMarch","NotOpenMarch")))

invisible(redis_con$BITOP("OR","ReceivedNoOpenButModified",c("ReceivedNoOpenButModifiedJanuary","ReceivedNoOpenButModifiedFebruary","ReceivedNoOpenButModifiedMarch")))
q6_users_who_received_didnt_open_and_modified <- redis_con$BITCOUNT("ReceivedNoOpenButModified")
print(paste('Users who received at least one mail, didn\'t open it and they updated their listing:',q6_users_who_received_didnt_open_and_modified), quote = FALSE)

# Question 1.7: Verify the effectiveness of the e-mail recommendation approach

# Inner join of 2 csv files
merged<-merge(x=emails, y=listings, by=c("UserID","MonthID"))

# E-mails opened per month with the listing to be modified
for(i in 1:nrow(merged))
{
  if ((merged$EmailOpened[i]==1) & (merged$ModifiedListing[i] ==1)) {
    if (merged$MonthID[i]==1) {
      redis_con$SETBIT("EmailsOpenedModifiedJanuary",merged$UserID[i],"1")
    } else if (merged$MonthID[i]==2) {
      redis_con$SETBIT("EmailsOpenedModifiedFebruary",merged$UserID[i],"1")
    } else {
      redis_con$SETBIT("EmailsOpenedModifiedMarch",merged$UserID[i],"1")
    }
  }
}

# E-mails opened per month without the listing to be modified
for(i in 1:nrow(merged))
{
  if ((merged$EmailOpened[i]==1) & (merged$ModifiedListing[i] ==0)) {
    if (merged$MonthID[i]==1) {
      redis_con$SETBIT("EmailsOpenedNotModifiedJanuary",merged$UserID[i],"1")
    } else if (merged$MonthID[i]==2) {
      redis_con$SETBIT("EmailsOpenedNotModifiedFebruary",merged$UserID[i],"1")
    } else {
      redis_con$SETBIT("EmailsOpenedNotModifiedMarch",merged$UserID[i],"1")
    }
  }
}

invisible(redis_con$BITCOUNT("EmailsOpenedModifiedJanuary"))
invisible(redis_con$BITCOUNT("EmailsOpenedNotModifiedJanuary"))
# Percentage of modified listings for January)
JanuaryPerc <- round(redis_con$BITCOUNT("EmailsOpenedModifiedJanuary") /(redis_con$BITCOUNT("EmailsOpenedModifiedJanuary")+redis_con$BITCOUNT("EmailsOpenedNotModifiedJanuary")) * 100,3)
print(paste('Percentage of January users who who opened and modified divided by all users who opened in January:',round(JanuaryPerc,2),'%'), quote = FALSE)

invisible(redis_con$BITCOUNT("EmailsOpenedModifiedFebruary"))
invisible(redis_con$BITCOUNT("EmailsOpenedNotModifiedFebruary"))
# Percentage of modified listings for February
FebruaryPerc <- round((redis_con$BITCOUNT("EmailsOpenedModifiedFebruary") / (redis_con$BITCOUNT("EmailsOpenedModifiedFebruary") + redis_con$BITCOUNT("EmailsOpenedNotModifiedFebruary"))) * 100,3)
print(paste('Percentage of February users who who opened and modified divided by all users who opened in February:',round(FebruaryPerc,2),'%'), quote = FALSE)

invisible(redis_con$BITCOUNT("EmailsOpenedModifiedMarch"))
invisible(redis_con$BITCOUNT("EmailsOpenedNotModifiedMarch"))
# Percentage of modified listings for March
MarchPerc <- round((redis_con$BITCOUNT("EmailsOpenedModifiedMarch") / (redis_con$BITCOUNT("EmailsOpenedModifiedMarch") + redis_con$BITCOUNT("EmailsOpenedNotModifiedMarch"))) * 100,3)
print(paste('Percentage of March users who who opened and modified divided by all users who opened in March:',round(MarchPerc,2),'%'), quote = FALSE)

# It make sense to keep sending email because appears that 4768 people update without open their email and 6247 appears to updated 
# after the opened their email.

# Close Redis
redis_con$FLUSHALL()
