## Planning by Claude
- read the requirement.md file and create plan.md file
- create skills with code convention
- use controller for handle request and response and service for business logic and repository pattern as
   skill where DB oriented operation will be handled
- Use unit and e2e  test cases and run each every test cases for any changes, fix if anything failed

## Implement by Codex
- I planned this by claude, now it's your responsibility to implement this project. Read plan.md file and .claude/architechture.md and .claude/convention.md file 
Implement this correctly
- Verify
    - is webhooks-ts library created with testing
    - Can it be use in the service with registration and emit
    - Can it be consume in consumer
    - Which directory is the webhook-ts library and how it will be packaged?
- Package the library and use it in the producer app and test the whole life cycle if consumer can consume?
- Organize the /src directory from the prespective of Software Architect who know how to structure reusable library for extendebility
- Now make sure 100% 
    - Library can be packaged like the industry standard
    - Producer app can import it and use it as defined in requirement.md file
    - Consumer app can get the webhook message correctly
- in src directory controller is unused in tests/unit is also. find and Remove all unused files and directories then run the test case 
- One architectural issue, not all consumer should get same message, producer should know which consumer will get what message. fix this and update the test case accordingly.
- test the full cycle from packaging -> import in producer -> consume the different message in two consumer to make sure no fan out
- Update the readme.md file properly, so an engineer can understand step by step from the packaging -> import -> how to use using docker -> how to test -> how sqlite will be accessed -> what data is persisting in sqlite -> how consumer app getting messages in which way.