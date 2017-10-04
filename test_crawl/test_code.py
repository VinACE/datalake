
l =["PRDSHPF_1","apapp100_2","apapp200_3","ararp100_4","ararp200_5","arccp200_6","glglp100_7","glsrp100_8","inpop100_9","inpop10h_10","inpop300_11","inpop30h_12","inpop400_13","kcdep100_14","kcfap100_15","kcfap200_16","kcfpp100_17","kcglp100_18","kcglp200_19","kcgmp100_20","kcgpp100_21","kcrap100_22","kcrap200_23","mflbp100_24","mfltp100_25","mftxp100_26","mfwop100_27","mfwop10h_28","mfwop300_29","mfwop30h_30","mfwop400_31","mscmp100_32","mspmp100_33","mspmpext_34","msvmp100_35","obcdp100_36","obcdp200_37","obcop100_38","obcop200_39","obcop300_40","obcopext_41","obcrp100_42","obcrpext_43","obirp111_44","obotp100_45","obstp100_46","obtmp100_47","obtop100_48","popvp100_49","potmp100_50","pspsp100_51","sasmp100_52"]

print(len(l))

for i in range(len(l)):
    print("CREATE TABLE " + l[i] + "(")
    print("\n \n \n")
    print("PRIMARY KEY ( ACTIV ));")
    print("\n \n \n")