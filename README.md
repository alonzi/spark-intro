# spark-intro
This repo contains materials for a spark introduction

## Who am I?
* [Senior Research Data Scientist with the Data Science Institute and UVA Library](https://dsi.virginia.edu/people/peter-alonzi)
* I like to be interrupted with questions! Please jump right in.

## Welcome to the UVA Library
* [Research Data Services](https://data.library.virginia.edu/)
* [Workshop Series](https://data.library.virginia.edu/training/)
  * Data Storage Best Practices (Bill Corey)	Tuesday, 11/6	14:00 – 15:30	Brown 133
  
## Getting Spark 
* [Amazon Web Services Sagemaker](https://us-east-1.signin.aws.amazon.com/oauth?SignatureVersion=4&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAJMOATPLHVSJ563XQ&X-Amz-Date=2018-10-15T15%3A37%3A38.105Z&X-Amz-Signature=16712fc21b036c069bd01eb7d77e0ea3bfc2db6308ea89c567b58c63f917f030&X-Amz-SignedHeaders=host&client_id=arn%3Aaws%3Aiam%3A%3A015428540659%3Auser%2Fhomepage&nc2=h_ct&redirect_uri=https%3A%2F%2Fconsole.aws.amazon.com%2Fconsole%2Fhome%3Fnc2%3Dh_ct%26src%3Dheader-signin%26state%3DhashArgs%2523%26isauthcode%3Dtrue&response_type=code&src=header-signin&state=hashArgs%23)
  * This link lets you sign into the AWS console
    * Account ID = open-data-lab
  * See the handout for IAM user name / password

## What is Spark?
"Apache Spark is an open-source distributed general-purpose cluster-computing framework." ~ wikipedia
  * cluster programming interface
  * data parallelization
  * v1 2014-05-26
  * v2 2016-07-26
  * current v2.4
  
# Goals for Today
1. Get everyone running on spark
2. Get comfortable with spark
3. Learn how to look up help

## Outline
1. AWS Sagemaker orientation
2. Data types
3. Loops
4. Logic
5. How to import (aka the most important part)

### A quick note
Today we are working on python. However there is some knowledge of programming that is required. Don't worry if you don't know it, please ask questions. I will do my best to answer them. And I will also do my best to indicate when something is a python specific detail and when it is a programming in general item.

## A brief history
* Designed by [Guido van Rossum](https://www.google.com/search?q=google+image+search+guido+van+rossum&safe=off&rlz=1C5CHFA_enUS690US690&source=lnms&tbm=isch&sa=X&ved=0ahUKEwjE_eGK6KHdAhXrRd8KHUzBDHsQ_AUICigB&biw=1440&bih=697)
* version 1.0 1994
* version 2.0 2000
* version 3.0 2008 (not widely adopted until a few years ago)
* [logo](https://www.google.com/search?q=python+logo&safe=off&rlz=1C5CHFA_enUS690US690&source=lnms&tbm=isch&sa=X&ved=0ahUKEwi9xN-J8aHdAhVBMt8KHT-WDEEQ_AUICigB&biw=1440&bih=697)
* [anaconda logo](https://www.google.com/search?q=anaconda+logo&safe=off&rlz=1C5CHFA_enUS690US690&source=lnms&tbm=isch&sa=X&ved=0ahUKEwin88Gf8aHdAhUhiOAKHeGLBHYQ_AUICigB&biw=1440&bih=697)

# Let's Get to It (hopefully everyone is done installing)
* open spyder [it looks like this](spyder.png)
  * text editor
  * variable explorer
  * console
  * control icons
  
## Strings
* A string is a 'string' of characters
  * 'apple'  # letters
  * 'blue42' # letters and numbers
  * 'i am the very model of a modern major general' # spaces are fine
  * '7 hills' # it can even start with a number
  
## Comments
We also introduce comments here, the computer will ignore everything after the '#' symbol. There are other forms but we'll see them later on.

## Variables
You can "save" things as variables. For those curious as to what's going on under the hood...in python a variable is actually just a pointer to the location in memory where the object lives.
* a = 'apple'
  * a is the variable
  * = is the assignment operator
  * 'apple' (a string) is the object assigned to a
  
* *Important Note*: the assignment operator is not like an equals sign
  * a=5
  * a=7
    * totally works, a was just reassigned to point to 7
    
## Functions
* print(a) # this function will show us what a points to
* we know that print is a function because there is no space between print and the "("
* format of a function: name(arguments)
* we say we "call" a function
* *this is super important* in python the way to spot a function is no space before a "(" and a letter or number
  * [python built-in functions](https://docs.python.org/3/library/functions.html)
  * print(...)
  * type(...)
  * pow(...)
  * in an equation you may see 5*(2+3), you won't seet 5(2+3) (try it and see what happens)
  
## Indexing
* for objects with an order you can access individual elements
* [indexing](indexing.png)
* you can also pull out slices
  * syntax [X:Y]
    * starting at X
    * upto but not including Y

## Lists
* represented like functions but with [...]
* there is an order to the items
* the items can be of any type

## Dictionary
* represented like lits but with {...}
* there is no order to items
* items contain two pieces: a key and a value represented key:value and the key must be a string

## loops - used when you want to repeat code
* for loop
  * for X in Y: << code >>
    * X is a new variable created on the spot
    * Y is some preexisting iterable
    * << code >> is a block of code you want to repeat
    * eg: for x in range(10): print(x)

* while loop
  * while Z: << code >>
    * Z is a boolean
    * << code >> is a block of code you want to repeat
    * eg: while i<10: print i; i+=1

## if statements
* example
  * if X: << code A >>
  * else: << code B >>
    * X is a boolean
    * << code A >> is some code
    * << code B >> is some code, could be the same
    


# Import
* *This is the most important topic*
* The import command let's you bring in code from another file and use it
* one example: random number generation
  * import numpy
  * numpy.random.randn()
* There are two steps [info on conda](https://conda.io/docs/user-guide/tasks/manage-pkgs.html) [info on pip](https://pip.pypa.io/en/stable/user_guide/)
  1. install the module, eg: shell]$ conda install numpy
  2. python>>> import numpy

# Scripting vs Programming
It's a matter of modularity. Programs are designed to be modular and work with other programs. Scripts are designed to be single use.



# Ways to Practice
1. Write some code
2. Ask a friend to review it

* Beginning
  * Flip a coin (~10 lines)
  * Play rock, paper, scissors  (~25 lines)
* Intermediate
  * Guess a secret number between 1 and 10. With hints. (~20 lines)
  * Dice rolling program
* Expert
  * Play blackjack
  * Play roulette







