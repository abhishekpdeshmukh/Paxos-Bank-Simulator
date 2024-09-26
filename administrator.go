package main

import (
	"fmt"
	"os"
)

func main() {

	// Infinite loop for continuously taking user input
	for {
		// Display menu options
		fmt.Println("\nSelect an option:")
		fmt.Println("1. Read Transaction SET on Default Path")
		fmt.Println("2. Read Transaction SET on Modified Path")
		fmt.Println("3. Kill Nodes")
		fmt.Println("4. Print Balance on Specific Server")
		fmt.Println("5. Spawn All Nodes")
		fmt.Println("6. Exit")

		// Read user's choice
		var option int
		fmt.Print("Enter your option: ")
		_, err := fmt.Scanln(&option)
		if err != nil {
			fmt.Println("Invalid input, please enter a number.")
			continue
		}

		// Use a switch-case to handle different options
		switch option {
		case 1:
			fmt.Println("Executing: Read Transaction SET on Default Path")
			// Call your function for this option here
		case 2:
			fmt.Println("Executing: Read Transaction SET on Modified Path")
			// Call your function for this option here
		case 3:
			fmt.Println("Executing: Kill Nodes")
			// Call your function for this option here
		case 4:
			fmt.Println("Executing: Print Balance on Specific Server")
			// Call your function for this option here
		case 5:
			fmt.Println("Executing: Spawn All Nodes")
			// Call your function for this option here
		case 6:
			fmt.Println("Exiting program...")
			os.Exit(0)
		default:
			fmt.Println("Invalid option. Please choose a valid number.")
		}
	}
}
