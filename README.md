# dist-systems-assignment-2
Brian Moyles - 21333461
Dave Szczesny - 

# Part 2 - Proof It cant Read in multiple files 
- Added a Test Variable to track the number of args
```
    int i = 0;
    for (String file : args) {
    i++;
    input.put(file, readFile(file));
    }
    System.out.printf("HEREHERE %d", i);
```
![alt text](image.png)

Note: HEREHERE was used as every word is printed in the terminal making it hard to track

# Part 3 - Timing 
- Timing was added to different parts of the file in various important places
- The time was captured at the start and end of parts, thus allowing us to calculate the time taken
- Timings ArrayList was used to store all the values then write them to a file at the end using a new function
- Example:
    ```
        long mapStart = System.currentTimeMillis();
        ... Map Functionality ...
        long mapEnd = System.currentTimeMillis();
        timings.add("Map Time: " + (mapEnd - mapStart) + " ms");
    ```
- Results for running with 10 txt files:
    ```
    File Read Time: 326 ms
    Brute Force Time: 471 ms
    Map Time: 434 ms
    Group Time: 198 ms
    Reduce Time: 110 ms
    Distributed Map Time: 131 ms
    Distributed Group Time: 134 ms
    Distributed Reduce Time: 5982 ms
    Distributed Total Time: 6249 ms
    Total Execution Time: 12389 ms
    ```