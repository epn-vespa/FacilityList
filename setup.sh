#!/bin/bash

user=$1

# Redirect armstrong Ollama to localhost
ssh -L localhost:11435:145.238.151.114:11434 $user@tycho.obspm.fr
