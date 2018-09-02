# Databricks notebook source

input_file_RDD=sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")
user_data_RDD=sc.textFile("/FileStore/tables/userdata.txt")
result=[]

def mutual_friend_mapper(line):
  user_friends=line.split("\t")
  users=user_friends[0]
  friends=user_friends[1].split(",");
  friends_second_list=list(friends)
  mutual_friend_list=[]
  if friends:
    for friend in friends:
      if(friend !=''):
        friends_second_list.remove(friend)
        friends_first_list=list(friends_second_list)
        if(int(friend)>int(users)):
          mutual_friend_list.append(((int(users),int(friend)),friends_first_list))
          friends_second_list.append(friend)
        else:
          mutual_friend_list.append(((int(friend),int(users)),friends_first_list))
          friends_second_list.append(friend)  
  return mutual_friend_list
friend_pair_RDD=input_file_RDD.flatMap(mutual_friend_mapper)
mutual_friends_RDD=friend_pair_RDD.reduceByKey(lambda list1,list2:list(set(list1).intersection(list2)))
mutual_friends_count_RDD=mutual_friends_RDD.mapValues(lambda x: len(x))
top_ten_mutual_friends_RDD=mutual_friends_count_RDD.map(lambda y: (y[1],y[0])).sortByKey(ascending=False).collect()[0:9]

user_data_two_RDD=user_data_RDD.map(lambda x:x.split(","))
friends_details_RDD=user_data_two_RDD.map(lambda x: (int(x[0]),x[1:]))
for x in top_ten_mutual_friends_RDD:
  friends_count_1=x[0]
  friends_each=x[1]
  friend_1=friends_details_RDD.lookup(friends_each[0])[0]
  friend_2=friends_details_RDD.lookup(friends_each[1])[0]
  fn_friend1=str(friend_1[0])
  ln_friend1=str(friend_1[1])
  fn_friend2=str(friend_2[0])
  ln_friend2=str(friend_2[1])
  address_friend1=str(friend_1[2])+" "+str(friend_1[3])+" "+str(friend_1[4])+" "+str(friend_1[5])+" "+str(friend_1[6])
  address_friend2=str(friend_2[2])+" "+str(friend_2[3])+" "+str(friend_2[4])+" "+str(friend_2[5])+" "+str(friend_2[6])
  result.append(str(friends_count_1)+'    '+fn_friend1+'    '+ln_friend1 +'    '+address_friend1+'              '+ fn_friend2+'    '+ln_friend2+'    '+address_friend2)
  sc.parallelize(result).collect()
result



# COMMAND ----------


