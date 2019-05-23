import mysql.connector
import csv

def main():
    Database_name = input("Type in your database's name:\n")
    Database_user = input("Type in your username:\n")
    Database_pass = input("Type in your password:\n")


    mydb = mysql.connector.connect(
        host="localhost",
        user=Database_user,
        passwd=Database_pass,
        database=Database_name
    )

    mycursor = mydb.cursor()

    user_input = input("What would you like to do? (1) Restart Database with new csv files or (2) Query: \n")

    if user_input == "1":
        mycursor.execute("drop table if exists odistances")
        mycursor.execute("drop table if exists skills")
        mycursor.execute("drop table if exists interests")
        mycursor.execute("drop table if exists users")
        mycursor.execute("drop table if exists organization")
        mycursor.execute("drop table if exists pconnections")
        mycursor.execute("create table users(id int, First text, Last text)")
        mycursor.execute("create table organization(U_ido int, name_o text, Type varchar(1))")
        mycursor.execute("create table pconnections(U_idp int, name_P text)")
        mycursor.execute("create table skills(U_ids int, name_s text, level_s int)")
        mycursor.execute("create table interests(U_idi int, name_i text, level_i int)")
        mycursor.execute("create table odistances(name1 text, name2 text, miles double)")
        mycursor.execute("load data local infile 'distance.csv' into table interests fields terminated by ',' enclosed by '\"' lines terminated by '\n' ignore 1 rows")
        mycursor.execute("load data local infile 'interest.csv' into table interests fields terminated by ',' enclosed by '\"' lines terminated by '\n' ignore 1 rows")
        mycursor.execute("load data local infile 'organization.csv' into table organization fields terminated by ',' enclosed by '\"' lines terminated by '\n' ignore 1 rows")
        mycursor.execute("load data local infile 'project.csv' into table pconnections fields terminated by ',' enclosed by '\"' lines terminated by '\n' ignore 1 rows")
        mycursor.execute("load data local infile 'skill.csv' into table skills fields terminated by ',' enclosed by '\"' lines terminated by '\n' ignore 1 rows")
        mycursor.execute("load data local infile 'user.csv' into table users fields terminated by ',' enclosed by '\"' lines terminated by '\n' ignore 1 rows")
        mydb.commit()
        main()


    elif user_input == "2":    
        further_input2 = input("Would you like to check: \n (1) users who share an interest and are near a specific type of user. \n (2) colleagues of colleagues of a user. \n (3) details and connections of a specific user.\n")
        if further_input2 == "1": #handles the query for users who share a skill or interest within a certain miles limit of a specific user
            query1 = input("please put the kind of organization the users work for (U, G or C)\n")
            query2 = input("now please put the maximum number of miles from the organization (example: 10)\n")
            mycursor.execute("drop table if exists temp_UO")
            mycursor.execute("create table temp_UO(tid int, First text, Last text, name_o text, type varchar(1))")
            mycursor.execute("insert into temp_UO select * from (select id, First, Last, name_o, type from users join organization on id = U_ido)A where type = %s", (query1,))
            mycursor2 = mydb.cursor(buffered = True)
            mycursor3 = mydb.cursor(buffered = True)
            mycursor2.execute("select tid, First, Last, name_o from temp_UO")
            for (tid, First, Last, name_o) in mycursor2:
                mycursor.execute("drop table if exists temp_O")
                mycursor.execute("create table temp_O(name1 text, name2 text, miles double)")
                mycursor.execute("insert into temp_O select * from (select odistances.name1 name1, odistances.name2 name2, odistances.miles miles from temp_UO join odistances on %s = odistances.name1 OR %s = odistances.name2)A where miles <= %s", (name_o, name_o, query2,))
                mycursor.execute("drop table if exists temp_U")
                mycursor.execute("create table temp_U(idu int, Firstu text, Lastu text)")
                mycursor.execute("insert into temp_U select * from (select id, First, Last from users join (select * from organization join temp_O on organization.name_o = temp_O.name1 OR organization.name_o = temp_O.name2 where U_ido != %s) A on users.id = A.U_ido)B", (tid,))
                mycursor3.execute("select * from temp_U")
                print ("User: %s %s, ID: %s \n Has these Users nearby with shared interests:\n Format is: ID, First Name, Last Name, Organization Name, List of Interests, List of Skills\n" % (First, Last, tid))
            
                for (idu, Firstu, Lastu) in mycursor3:
                    mycursor.execute("drop table if exists Loop_temp")
                    mycursor.execute("create table Loop_temp(id int, First text, Last text, name_o text)")
                    mycursor.execute("insert into Loop_temp select * from (select id, First, Last, name_o from users join organization on id = U_ido)A where id = %s" , (idu,))
                    mycursor.execute("alter table Loop_temp add Interests text")
                    mycursor.execute("update Loop_temp set Loop_temp.Interests = (select group_concat(name_I) AS name_I from (select name_I from Loop_temp join interests on id = U_idi)A)")
                    mycursor.execute("alter table Loop_temp add Skills text")
                    mycursor.execute("update Loop_temp set Loop_temp.Skills = (select group_concat(name_S) AS name_S from (select name_S from Loop_temp join skills on id = U_ids)A)")
                
                    mycursor.execute("select * from Loop_temp")
                    records = mycursor.fetchall()
                    print(records)
                    mycursor.execute("drop table if exists Loop_temp")
        
            mycursor.execute("drop table if exists temp_O")
            mycursor.execute("drop table if exists temp_U")
            mycursor.execute("drop table if exists temp_UO")
            mycursor2.close()
            mycursor3.close()
            mycursor.close()
            mydb.commit()
            mydb.close()
            mydb.disconnect()
                


        

        elif further_input2 == "2": #handles the query for colleague of colleagues of a user
            query1 = input("Please provide the user's id (1, 2, 3 ,etc)\n")

            mycursor.execute("drop table if exists temp_UO")
            mycursor.execute("create table temp_UO (id int, First text, Last text)")
            mycursor.execute("insert into temp_UO select id, First, Last from users where id = %s", (query1,)) 
            mycursor.execute("drop table if exists temp_P")
            mycursor.execute("create table temp_P (U_idp int, name_P text)")
            mycursor.execute("insert into temp_P select * from (select U_idp, name_P from pconnections join temp_UO on id = U_idp)A")
            mycursor.execute("drop table if exists temp_P2")
            mycursor.execute("create table temp_P2 (U_idp1 int, name_P text, U_idp2 int)")
            mycursor.execute("insert into temp_P2 select temp_P.U_idp U_idp1, temp_P.name_P name_P, pconnections.U_idp U_idp2 from temp_p join pconnections on temp_P.name_P = pconnections.name_P AND temp_P.U_idp != pconnections.U_idp")
            mycursor.execute("drop table if exists temp_P")
            mycursor.execute("create table temp_P(U_idp2 int, name_P text)")
            mycursor.execute("insert into temp_P select U_idp2, pconnections.name_P from temp_P2 join pconnections on U_idp2 = U_idp")
            mycursor.execute("drop table if exists temp_P2")
            mycursor.execute("create table temp_P2 (U_idp2 int, name_P text, U_idp3 int)")
            mycursor.execute("insert into temp_P2 select U_idp2, pconnections.name_P name_P, pconnections.U_idp U_idp3 from temp_P join pconnections on temp_P.name_P = pconnections.name_P AND U_idp2 != U_idp")
            mycursor.execute("drop table if exists answers")
            mycursor.execute("create table answers (id int, First text, Last text)")
            mycursor.execute("insert into answers select id, First, Last from (select distinct U_idp3 from temp_P2) as B join users on B.U_idp3 = users.id")
            mycursor.execute("select * from answers")
            records = mycursor.fetchall()
            print("Colleagues of Colleagues of the user:\n ID, First Name, Last Name")
            for row in records:
                print(row)
        
            mycursor.execute("drop table if exists temp_P")
            mycursor.execute("drop table if exists temp_P2")
            mycursor.execute("drop table if exists temp_UO")

            mydb.commit()
            mycursor.close()
            mydb.close()
            mydb.disconnect()

        elif further_input2 == '3': #handles query to get all data on specified user
            query1 = input("Please provide the user's id (1, 2, 3, etc)\n")
            mycursor.execute("drop table if exists temp_UO")
            mycursor.execute("create table temp_UO(id int, First text, Last text, name_o text, type varchar(1))")
            mycursor.execute("insert into temp_UO select * from (select id, First, Last, name_o, type from users join organization on id = U_ido)A where id = %s", (query1,))
            mycursor.execute("alter table temp_UO add Projects text")
            mycursor.execute("update temp_UO set temp_UO.Projects = (select group_concat(name_P) AS name_P from (select name_P from temp_UO join pconnections on id = U_idp) A)")
            mycursor.execute("alter table temp_UO add Interests text")
            mycursor.execute("update temp_UO set temp_UO.Interests = (select group_concat(name_I) AS name_I from (select name_I from temp_UO join interests on id = U_idi)A)")
            mycursor.execute("alter table temp_UO add Skills text")
            mycursor.execute("update temp_UO set temp_UO.Skills = (select group_concat(name_S) AS name_S from (select name_S from temp_UO join skills on id = U_ids)A)")
            mycursor.execute("select * from temp_UO")
            records = mycursor.fetchall()
            print("Details of the User:\nID, First Name, Last Name, Organization, Organization Type, Projects, Interests, Skills")
            print(records)
            mycursor.execute("drop table if exists temp_UO")

            mydb.commit()
            mycursor.close()
            mydb.close()
            mydb.disconnect()
    
        else:
            print("unimplemented query")

    else:
        print("incorrect input")

if __name__ == "__main__":
    main()