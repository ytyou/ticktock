#!/bin/bash

TT_HOME=$HOME/src/tt

/usr/bin/cp -rf $TT_HOME/admin $HOME/backup/
/usr/bin/cp -rf $TT_HOME/conf $HOME/backup/
/usr/bin/cp -rf $TT_HOME/scripts $HOME/backup/
/usr/bin/cp -rf $TT_HOME/include $HOME/backup/
/usr/bin/cp -rf $TT_HOME/src $HOME/backup/

exit 0
