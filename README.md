# QUERY SERVER

Use `python run_server.py` to start the server from this directory.

By default, the server is available at all interfaces on port 9000.

Flags for Theano to use GPU for LSTM model: `THEANO_FLAGS='device=gpu,floatX=float32'`

# Train LSTM model
First, adjust the location paths in train_st_model.py to what you need, then:
`THEANO_FLAGS='device=gpu,floatX=float32' python3 train_st_model.py`

# Best Practices

**Always:**

1. Create a new branch when working on a new feature/fix. Then merge to master.

2. Leave comments in your code and a short description of what your script does in the beginning.

3. If you making a new folder add a README.md that describes the file structure.

4. Update the wiki with what you put in the README. (file structure, descriptions etc.)

## General practical stuff
To add a new feature/fix start first start a new branch

``` git branch <branch_name>```

Checkout to that branch

``` git checkout <branch_name>```

When you are done, merge your branch to master

```git checkout master``` 

```git merge <branch_name>```

Make your commit descriptions informative! If you have some uncommitted changes but you want to pull just do
```git commit -am "WIP"``` and then pull.

If you are pulling and you want to tell git to keep your changes in case of a conflict use

```git pull -s recursive -X ours --no-edit origin <branch_name>```




