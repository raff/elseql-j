package org.aromatic.elseql;

import java.io.*;
import java.util.*;

import jline.ArgumentCompletor;
import jline.ConsoleReader;
import jline.SimpleCompletor;

public class ElseShell {
    private String[] commandsList = { "help", "select", "exit", "quit" };
    private String prompt = "elseql> ";

    public ElseShell() {
    }

    public void run() throws IOException {
        ConsoleReader reader = new ConsoleReader();
        reader.setBellEnabled(false);
        List completors = new LinkedList();

        completors.add(new SimpleCompletor(commandsList));
        reader.addCompletor(new ArgumentCompletor(completors));

        String line;

        while ((line = readLine(reader)) != null) {
            if ("help".equals(line)) {
                printHelp();
            }
	    
	    else if ("select".equals(line)) {
                System.out.println("do select");
            }
	    
	    else if ("exit".equals(line) || "quit".equals(line)) {
                break;
            }
	    
	    else {
                System.out.println("Invalid command. For assistance press TAB or type \"help\"");
            }

            System.out.flush();
        }

	System.out.println("Goodbye!");
    }

    private void printHelp() {
        System.out.println("help        - Show help");
        System.out.println("select      - Select statement");
        System.out.println("exit/quit   - Exit application");
    }

    private String readLine(ConsoleReader reader) throws IOException {
        String line = reader.readLine(prompt);
	if (line != null)
	    line = line.trim();

        return line;
    }

    public static void main(String[] args) throws IOException {
        ElseShell shell = new ElseShell();
        shell.run();
    }
}
