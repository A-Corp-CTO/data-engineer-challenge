--- Create view with only accounts with no activity or no activity within 2 years
DROP VIEW IF EXISTS DealsWithNoActivity
CREATE VIEW DealsWithNoActivity
AS
SELECT Deals.*, LastActivities.LastActivity from Deals
LEFT JOIN (
	SELECT DealID, MAX(MarkedAsDone) as LastActivity from Activities
	GROUP BY DealID
) as LastActivities on Deals.DealID = LastActivities.DealID
WHERE LastActivities.LastActivity IS NULL OR LastActivities.LastActivity <='2021-04-28'