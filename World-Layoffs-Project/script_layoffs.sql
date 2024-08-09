-- Data cleaning 

-- 1 Remove duplicates
-- 2 Standarize data
-- 3 Remove Null or Blanck values

-- First we create a staging table to mantain the integrity of the raw data and work in it.
CREATE TABLE layoffs_s LIKE layoffs;
INSERT layoffs_s SELECT * FROM layoffs;

-- 1 Remove duplicates
	WITH table_1 as(
		SELECT ROW_NUMBER() OVER(
						PARTITION BY company, location, industry, 
									total_laid_off,percentage_laid_off,
									`date`,stage, country,funds_raised_millions	
								) AS row_numb,	
								layoffs_s.*  
		FROM layoffs_s)
		DELETE FROM layoffs_s 
		WHERE (company, location, industry, 
				total_laid_off,percentage_laid_off,
				`date`,stage, country,funds_raised_millions) 
					IN (
						SELECT company, location, industry, 
								total_laid_off,percentage_laid_off,
								`date`,stage, country,funds_raised_millions 
						FROM table_1 
						WHERE row_numb>1);
						
-- 2 Standarize data
	SELECT DISTINCT
		country
	FROM
		layoffs_s
	ORDER BY 1;
	-- Noise detected 'United States' and 'United States.'
			UPDATE layoffs_s 
			SET 
				country = 'United States'
			WHERE
				country LIKE ('United States%');
	   
	-- Transform text like dates in date sql data type    
			SELECT 
				`date`
			FROM
				layoffs_s
			ORDER BY 1;

			UPDATE layoffs_s 
			SET 
				`date` = STR_TO_DATE(`date`, '%m/%d/%Y');
				
			ALTER TABLE layoffs_s
			MODIFY COLUMN `date` DATE;

-- 3 Remove Null or Blanck values

	-- Industry
			-- Filling the blank or null industry names with the values of companys 
            -- with the same name
			SELECT 
				l1.company,l1.industry,l2.industry
			FROM
				layoffs_s as l1 join layoffs_s as l2
				on l1.company =l2.company
			WHERE
				(l1.industry IS NULL OR l1.industry = '')
				and (l2.industry IS NOT NULL AND l2.industry != '');

			UPDATE 
				layoffs_s as l1 join layoffs_s as l2
				on l1.company =l2.company
			SET 
				l1.industry = l2.industry
			WHERE
				(l1.industry IS NULL OR l1.industry = '')
				and (l2.industry IS NOT NULL AND l2.industry != '');
				
			SELECT total_laid_off FROM layoffs_s;

			-- Bally's Interactive without any match ('Gaming' --> from Google :))
			select company, industry from layoffs_s where industry IS NULL OR industry = ''; 
			update layoffs_s set industry = 'Gaming' where company = "Bally's Interactive";


	-- total_laid_off
   
		SELECT t2.numb/t1.numb as percentage FROM 
			(SELECT count(*) numb FROM layoffs_s) AS t1 
            JOIN
			(SELECT count(*) numb FROM layoffs_s 
            WHERE total_laid_off IS NULL OR total_laid_off = '') as t2;
            
			-- arround 30% of total_laid_off are nulls, we can either delete this rows or ... 
			-- fillthem with the avg value of the column, since 0.3 is not that near to one ..
			--  we are going to do the second procedure
					SET @avg_total_laid_off = (
						SELECT DISTINCT 
							AVG(total_laid_off) OVER() AS numb 
						FROM layoffs_s
					);
					UPDATE layoffs_s 
					SET total_laid_off = @avg_total_laid_off
					WHERE total_laid_off IS NULL;


	-- percentage_laid_off
			SELECT t2.numb/t1.numb as percentage FROM 
			(SELECT count(*) numb FROM layoffs_s) AS t1 
            JOIN
			(SELECT count(*) numb FROM layoffs_s 
            WHERE percentage_laid_off IS NULL OR percentage_laid_off = '') as t2;
            
			SELECT * FROM layoffs_s 
            WHERE percentage_laid_off IS NULL OR percentage_laid_off = '';
			-- Same circunstances, but this time we are going to fill the null values with the average ..
			-- of the layoffs of the same country and year

					CREATE TEMPORARY TABLE tabla1 
					SELECT country,year,avg(avg) as avg 
					FROM 
					(SELECT 
						country,
						YEAR(`date`) AS year,
						layoffs_s.percentage_laid_off as avg
					FROM layoffs_s 
					WHERE percentage_laid_off IS NOT NULL
					ORDER BY country,year) as t1
					GROUP BY country,year
					ORDER BY country,year;

					UPDATE layoffs_s join tabla1 set layoffs_s.percentage_laid_off = tabla1.avg
					WHERE layoffs_s.country = tabla1.country and YEAR(layoffs_s.`date`) = tabla1.year;

					-- After this update, some rows remain with null data, 
					-- this is because there were no other rows from the same country and year with non-null values,
					-- taking into account that there are very few, it is best to delete them

							DELETE FROM layoffs_s WHERE percentage_laid_off is null;
                            
                            
			-- date 
					SELECT * FROM layoffs_s WHERE `date` IS NULL;
                    -- just one, so..
                    DELETE FROM layoffs_s WHERE `date` IS NULL;
                    
                    
			-- stage
					SELECT * FROM layoffs_s WHERE stage IS NULL OR stage = '';
					SELECT DISTINCT stage FROM layoffs_s;
                    -- Since we have an 'Unknown' value in stage column, I am just going to replace null values ​​with that
							UPDATE layoffs_s SET stage = 'Unknown' WHERE stage is null or stage = '';
			
            
            -- country
					select * from layoffs_s where country is null or country = '';
                    -- None
			
            
            -- funds_raised_millions
					SELECT * FROM layoffs_s WHERE funds_raised_millions IS NULL;
                    SELECT t2.numb/t1.numb as percentage FROM 
						(SELECT count(*) numb FROM layoffs_s) AS t1 
						JOIN
						(SELECT count(*) numb FROM layoffs_s 
						WHERE funds_raised_millions IS NULL) as t2;
                    -- Only 9%, deleting...
                    DELETE FROM layoffs_s WHERE funds_raised_millions IS NULL;
                    
                    
	-- Amount of losses resulting from delete statements
			 SELECT t2.numb/t1.numb FROM 
					(SELECT count(*) numb FROM layoffs) AS t1 
                    JOIN
					(SELECT count(*) numb FROM layoffs_s) AS t2;
			-- 11% of data where lost