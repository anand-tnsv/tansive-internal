I am going to give you a small project.
Look at the attached files where I have implemented a set of REST APIs for CRUD. See the objectcrud_test.go to understand how these APIs have to be called. The implementation of the CRUD commands are in the internal/catalogsrv/apis folder. Take a look at them if you have to.

Now, can you implement the cobra command file that you had previously generated in internal/cli/commands.go?
I would prefer that your implement the generic http client part of it as a utility function where I can add API keys to the header, sign headers etc at a later point. Create separate files in internal/cli folder if you have to.
Here's something to note -
Users will provide a yaml file as input. the command needs to convert the yaml to json and send it to the server. The cli should feel like kubectl - create -f will create it and return an error if already exists. apply will trying to create, if error, it'll try to update it. You need only 4 functions for CRUD. And this should ideally handle all cases. When the user provides the yaml file, you read the yaml, convert to json and look for the Kind and it should tell you the kind of the resource and whether it's a catalog, variant, workspaces etc.

Catalog is at the root level. There are many variants in a catalog. Many namespaces in a variant. Many workspaces in a variant. The yaml file, when asking to create say a namspace may or may not refer to the variant and catalog. However, the user can specify the variant, catalog and namespaces in the command line via --catalog, --variant, --namespace, --workspace or their shorthand -c, -v, -n, -w. If they provide these options then add those as query strings in the URL. See if you can do that generically in one utility function.

For this project, let's start with cli create -f some-file.yaml . Implement the create command. We'll do the rest one by one.
