


def main():

    inf = open("bobaadr.txt","r")
    outf = open("binmap.txt","w")

    inf.readline()  #flush first line

    BBL = ""
    BIN = ""
    for line in inf:
        if line[1]=="3":
            BBL = "3"+line[5:10]+line[13:17]
            BIN = line[20:27]
            outf.write(BBL+","+BIN+"\n")

    inf.close()
    outf.close()

    return 0


main()
