package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	hdfs "github.com/colinmarc/hdfs"
	crypto "luc.core/crypto"
	keygen "luc.core/keygen"
)

func main() {
	//connect to the name node of hdfs
	hdfsConn, err := hdfs.New("192.168.2.104:8020")

	//get a file hadler of the input file
	ihdfsFile, err := hdfsConn.Open("/user/aria/pipeline/luhs/clarity_2018-06-26_11_08_20")

	//ihdfsFile, err := os.Open("/home/aria/input/aria/pipeline_in/lumcdlsql01/clarity/N/1/COPYING__batch4")

	if err != nil {
		fmt.Println("got err when try openning the input file")
		os.Exit(1)
	} else {

		defer ihdfsFile.Close()

		// get the kay from daemon with a valid secret key
		dKey, _ := keygen.CheckDaemon()

		if dKey == "0" {
			fmt.Println("ebklasdjfasd")
			os.Exit(1)
		}

		//get a file handler of the output file
		ohdfsFileName := "/user/aria/repository/luhs/" + ihdfsFile.Name() + ".tag"
		ohdfsFile, err := hdfsConn.Create(ohdfsFileName)

		if err != nil {
			fmt.Println("got err when try creating the output file")
			os.Exit(1)
		}
		writer := bufio.NewWriter(ohdfsFile)

		defer ohdfsFile.Close()

		// Start reading from the input file with a reader.
		reader := bufio.NewReader(ihdfsFile)
		var line string // var for lines read in
		var tsep = "|"  //token separator

		for {
			fmt.Println("==================")

			// Read next lines
			line, err = reader.ReadString('\n')

			// check for errors or end-of-file (EOF) condition
			//todo: testing for a line without \n at the end of the file
			if err == io.EOF {
				fmt.Printf(" > Reached EOF %v\n", err)
				break
			}

			// check of error conditions, other than EOF
			if err != nil {
				fmt.Printf(" > Readline Error: %v\n", err)
				break
			}

			var tposuuid = 0 // extract uuid token at this position
			var uuid = ""
			var tposresdt = 8 // extract results_dt token at this position
			var resdt = ""
			var tposphi = 11 // extract phi token at this position
			var phi = ""
			var tposnote = 12 // extract note_text token at this position
			var note = ""

			// Split lines into tokens
			result := strings.Split(line, tsep)

			// Display all elements.
			for i := range result {
				fmt.Println(strconv.Itoa(i) + ":  " + result[i])

				//settting up needed fields for phi scrubbing process
				switch i {
				case tposuuid:
					uuid = strings.TrimSpace(result[i])
				case tposphi:
					phi = strings.TrimSpace(result[i])
				case tposresdt:
					resdt = strings.Replace(strings.Replace(strings.TrimSpace(result[i]), "T", " ", -1), "Z", "", -1)
				case tposnote:
					note = strings.TrimSpace(result[i])
				}

				//writing the data elements back to output file except note_text field
				if i != tposnote {
					fmt.Println("writing to file " + strconv.Itoa(i) + ":  " + strings.TrimSpace(result[i]) + tsep)
					//add an empty element if it is missing from the input file.
					if len(strings.TrimSpace(result[i])) == 0 {
						writer.WriteString("" + tsep)
					} else {
						writer.WriteString(strings.TrimSpace(result[i]) + tsep)
					}
				}
			}

			var tempValue = "" //temp value to parse out phi infos
			var phiKey = ""
			var phiValue = ""
			var maskedNote = ""

			individualKey, _ := keygen.GenerateKey(resdt, dKey)
			decryptedPhi, _ := crypto.Decrypt(individualKey, uuid, phi)
			decryptedNote, _ := crypto.Decrypt(individualKey, uuid, note)

			fmt.Println("individualKey: " + individualKey)
			fmt.Println("decryptedPhi: " + decryptedPhi)
			fmt.Println("decryptedNote: " + decryptedNote)

			if len(strings.TrimSpace(decryptedPhi)) > 0 {
				resultphi := strings.Split(decryptedPhi, "~")

				for j := range resultphi {
					tempValue = strings.TrimSpace(resultphi[j])

					if strings.Contains(tempValue, ":") {
						phiData := strings.SplitN(tempValue, ":", 2)
						phiKey = "<" + phiData[0] + ">"
						phiValue = phiData[1]
						//phiValue = "Phillip J. DeChristopher" //testing value for now

						if len(strings.TrimSpace(phiValue)) > 0 {
							if strings.Contains(decryptedNote, phiValue) {
								maskedNote = strings.Replace(decryptedNote, phiValue, phiKey, -1)
								fmt.Println("")
								fmt.Println("maskedNote: " + maskedNote)
								fmt.Println("")
							}
						}
					}
				}
			}

			// write the note to the output file, if there is maskedNote, encrypting it first.
			if len(strings.TrimSpace(maskedNote)) > 0 {
				encryptedNote, _ := crypto.Encrypt(individualKey, uuid, maskedNote)
				writer.WriteString(strings.TrimSpace(encryptedNote) + "\n")
			} else {
				writer.WriteString(note + "\n")
			}

			fmt.Println("")
			fmt.Println(len(result))
			fmt.Println("==================")
			writer.Flush()
		}
	}
}
