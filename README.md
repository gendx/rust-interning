# Simple program to optimize open data from RATP's disruptions API

[![Build Status](https://github.com/gendx/rust-interning/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/gendx/rust-interning/actions/workflows/build.yml)

The goal of this program implemented in Rust is to illustrate how applying the [interning pattern](https://en.wikipedia.org/wiki/Interning_(computer_science)) to a time series obtained from open data (the [disruptions API](https://prim.iledefrance-mobilites.fr/en/apis/idfm-disruptions_bulk) of the Paris public transport network RATP) can decrease storage needs by a significant amount.
