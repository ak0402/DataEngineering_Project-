Create Procedure UpdateWatermarkTable
 @lastload varchar(2000)

As 
BEGIN 
  -- Start the transaction 
 Begin Transaction;
  
  -- Update the incremental column in the table
  Update water_table
  Set last_load = @lastload
 Commit Transaction;
 End;
