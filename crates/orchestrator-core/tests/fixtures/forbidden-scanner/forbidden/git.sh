#!/bin/sh
git branch \
  | grep autospec \
  | xargs git branch -D
