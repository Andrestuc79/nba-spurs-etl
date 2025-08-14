
  create view "nba"."raw"."spurs_games__dbt_tmp"
    
    
  as (
    version: 2
sources:
  - name: raw
    tables:
      - name: spurs_games
  );