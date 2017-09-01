# r-

#==============load packages===========================
options(warnings=1)
setwd('C:/Users/guiyanyan/Desktop/工作资料/手机贷/贴吧数据情感分析')
timestart <- Sys.time()
library('jiebaR')
library(RMySQL)
library(plyr)
library(wordcloud2)
#====================read data from mysql======================================
mycon <- dbConnect(MySQL(),dbname="decision",username ="risk",password="risk.1234",host = "192.168.1.4",port =3306)
#dbSendQuery(mycon,"SET NAMES gbk")  #==="set names utf8" not work 
#tieba_chat <- dbGetQuery(mycon, "select reply_content,reply_time from cg_sjd_tieba_reply_type where length(reply_content)>3 and reply_time > '2017-05-31' ;") 
msjr_data <- read.csv('msjr.csv',stringsAsFactors = F)
dim(msjr_data)

## 删除???
dbGetQuery(mycon,'drop table if exists msjr_word_cloud')
dbGetQuery(mycon,'drop table if exists msjr_tieba_neg_emotion')
dbGetQuery(mycon,'drop table if exists msjr_tieba_emotion_final')

##分词
getwd()
engine <- worker(user='user.txt',stop_word = 'stop.txt')

msjr_segwords <- sapply(msjr_data$reply_content, segment, engine)
length(msjr_segwords)
msjr_segwords[1:10]
msjr_wf <- unlist(msjr_segwords)
msjr_wf <- as.data.frame(table(msjr_wf),stringsAsFactors = F)
msjr_wf <- msjr_wf[order(-msjr_wf$Freq),]
msjr_dele_one_wf <- msjr_wf[-which(nchar(msjr_wf$msjr_wf)==1),]
msjr_wf
msjr_dele_one_wf


##情感分析
#添加词典
msjr_emotion <- read.csv('emotion_dictionary.csv',stringsAsFactors = F)
typeof(msjr_emotion)
pos <- msjr_emotion$positive;neg <- msjr_emotion$negative
#================自定义情感函???
fun <- function(x,y) x %in% y
getEmotionalType <- function(x,pwords,nwords){ 
  pos.weight = sapply(llply(x,fun,pwords),sum)
  neg.weight = sapply(llply(x,fun,nwords),sum)
  total = pos.weight - neg.weight*3
  return(data.frame(pos.weight, neg.weight, total)) 
}


msjr_score <- getEmotionalType(msjr_segwords, pos, neg)


msjr_emotion <- cbind(msjr_data,msjr_score)

msjr_emotion$type[msjr_emotion$total <0] <- 'neg'
msjr_emotion$type[msjr_emotion$total >0] <- 'pos'
msjr_emotion$type[msjr_emotion$total ==0] <- 'neutral'



msjr_dele_one_wf$id <- c(1:nrow(msjr_dele_one_wf))

msjr_emotion[1:10,]

dbWriteTable(mycon,"msjr_tieba_neg_emotion",msjr_emotion)  
dbWriteTable(mycon,'msjr_word_cloud',dele_one_wf[1:100,])


msjr_tieba_emotion_final <- dbGetQuery(mycon, "SELECT b.date,b.cnt/a.cnt AS neg_rate FROM (SELECT DATE(reply_time) AS DATE,COUNT(*) AS cnt FROM cg_msjr_tieba_neg_emotion 
                                              WHERE DATE(reply_time) >'2017-06-31' GROUP BY DATE(reply_time)) AS a 
                                             JOIN (SELECT DATE(reply_time) AS DATE,TYPE,COUNT(*)AS cnt FROM cg_msjr_tieba_neg_emotion WHERE TYPE='neg' AND  DATE(reply_time) >'2017-06-31' GROUP BY DATE(reply_time),TYPE)
as b on a.date=b.date")

msjr_tieba_emotion_final[1:100,]
# tieba_emotion_final[c(1,2,5,8),2] <- c(0.26,0.33,0.225,0.333)
# 删除一些额外的停用词
msjr_dele_one_wf=msjr_dele_one_wf[-which(msjr_dele_one_wf$msjr_wf=='签到'),]
msjr_dele_one_wf=msjr_dele_one_wf[-which(msjr_dele_one_wf$msjr_wf=='马上'),]
msjr_dele_one_wf=msjr_dele_one_wf[-which(msjr_dele_one_wf$msjr_wf=='水水水'),]
msjr_dele_one_wf=msjr_dele_one_wf[-which(msjr_dele_one_wf$msjr_wf=='来自'),]
msjr_dele_one_wf=msjr_dele_one_wf[-which(msjr_dele_one_wf$msjr_wf=='一键'),]
msjr_dele_one_wf=msjr_dele_one_wf[-which(msjr_dele_one_wf$msjr_wf=='金融'),]
msjr_dele_one_wf=msjr_dele_one_wf[-which(msjr_dele_one_wf$msjr_wf=='定时'),]
msjr_dele_one_wf=msjr_dele_one_wf[-which(msjr_dele_one_wf$msjr_wf=='十五'),]
msjr_dele_one_wf=msjr_dele_one_wf[-which(msjr_dele_one_wf$msjr_wf=='看看'),]
 
msjr_dele_one_wf=msjr_dele_one_wf[1:100,]
msjr_dele_one_wf$id=c(1:100)
msjr_dele_one_wf[1:10,]
wordcloud2(msjr_dele_one_wf)

# 写入文件到测试库
dbWriteTable(mycon,"msjr_word_cloud",msjr_dele_one_wf) 

dbWriteTable(mycon,"msjr_tieba_emotion_final",msjr_tieba_emotion_final) 
timesend <- Sys.time()
runningtime<-timesend-timestart
print(runningtime)
print('DONE!')

