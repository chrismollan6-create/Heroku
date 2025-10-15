const express = require('express');
const jsforce = require('jsforce');
const app = express();

app.use(express.json());

// Environment variables (set in Heroku)
const SF_LOGIN_URL = process.env.SF_LOGIN_URL || 'https://login.salesforce.com';
const SF_USERNAME = process.env.SF_USERNAME;
const SF_PASSWORD = process.env.SF_PASSWORD;
const SF_SECURITY_TOKEN = process.env.SF_SECURITY_TOKEN;

// In-memory batch storage
let eventBatch = [];
const BATCH_SIZE = 100;
const BATCH_TIMEOUT = 60000; // 1 minute
let batchTimer = null;

// SendGrid webhook endpoint
app.post('/webhook/sendgrid', async (req, res) => {
  try {
    const events = req.body;
    
    if (!Array.isArray(events)) {
      return res.status(400).send('Invalid payload');
    }

    console.log(`Received ${events.length} events from SendGrid`);

    // Add events to batch
    events.forEach(event => {
      eventBatch.push({
        email: event.email,
        eventType: event.event,
        timestamp: event.timestamp,
        url: event.url || null,
        reason: event.reason || null,
        sg_message_id: event.sg_message_id,
        sg_event_id: event.sg_event_id
      });
    });

    // Check if batch is ready to process
    if (eventBatch.length >= BATCH_SIZE) {
      clearTimeout(batchTimer);
      processBatch();
    } else if (!batchTimer) {
      batchTimer = setTimeout(processBatch, BATCH_TIMEOUT);
    }

    res.status(200).send('OK');
    
  } catch (error) {
    console.error('Webhook error:', error);
    res.status(500).send('Internal error');
  }
});

// Process batched events and send to Salesforce
async function processBatch() {
  if (eventBatch.length === 0) return;

  const batch = [...eventBatch];
  eventBatch = [];
  batchTimer = null;

  console.log(`Processing batch of ${batch.length} events`);

  try {
    const conn = new jsforce.Connection({ loginUrl: SF_LOGIN_URL });
    await conn.login(SF_USERNAME, SF_PASSWORD + SF_SECURITY_TOKEN);
    console.log('Connected to Salesforce');

    // Group events by SendGrid Message ID for EmailMessage updates
    const eventsByMessageId = {};
    const eventsByEmail = {};
    
    batch.forEach(event => {
      // Track by message ID for individual email tracking
      if (event.sg_message_id) {
        if (!eventsByMessageId[event.sg_message_id]) {
          eventsByMessageId[event.sg_message_id] = {
            messageId: event.sg_message_id,
            email: event.email,
            opens: [],
            clicks: [],
            bounce: null
          };
        }

        const msgData = eventsByMessageId[event.sg_message_id];
        
        if (event.eventType === 'open') {
          msgData.opens.push({
            timestamp: new Date(event.timestamp * 1000)
          });
        } else if (event.eventType === 'click') {
          msgData.clicks.push({
            timestamp: new Date(event.timestamp * 1000),
            url: event.url
          });
        } else if (event.eventType === 'bounce' || event.eventType === 'dropped') {
          const bounceDate = new Date(event.timestamp * 1000);
          if (!msgData.bounce || bounceDate > msgData.bounce.timestamp) {
            msgData.bounce = {
              timestamp: bounceDate,
              reason: event.reason
            };
          }
        }
      }

      // Track by email for Lead/Contact aggregate updates
      if (!eventsByEmail[event.email]) {
        eventsByEmail[event.email] = {
          email: event.email,
          totalOpens: 0,
          totalClicks: 0,
          lastOpenDate: null,
          lastClickDate: null,
          lastBounceDate: null,
          bounceReason: null
        };
      }

      const emailData = eventsByEmail[event.email];
      const eventDate = new Date(event.timestamp * 1000);

      if (event.eventType === 'open') {
        emailData.totalOpens++;
        if (!emailData.lastOpenDate || eventDate > emailData.lastOpenDate) {
          emailData.lastOpenDate = eventDate;
        }
      } else if (event.eventType === 'click') {
        emailData.totalClicks++;
        if (!emailData.lastClickDate || eventDate > emailData.lastClickDate) {
          emailData.lastClickDate = eventDate;
        }
      } else if (event.eventType === 'bounce' || event.eventType === 'dropped') {
        if (!emailData.lastBounceDate || eventDate > emailData.lastBounceDate) {
          emailData.lastBounceDate = eventDate;
          emailData.bounceReason = event.reason;
        }
      }
    });

    // === PART 1: Update EmailMessage records ===
    const messageIds = Object.keys(eventsByMessageId);
    
    if (messageIds.length > 0) {
      // Query EmailMessage records by MessageIdentifier (stores SendGrid message ID)
      const emailMessages = await conn.query(
        `SELECT Id, MessageIdentifier, Open_Count__c, Click_Count__c, 
         First_Opened_Date__c, Last_Clicked_Date__c, Links_Clicked__c,
         Bounce_Date__c, Bounce_Reason__c
         FROM EmailMessage 
         WHERE MessageIdentifier IN ('${messageIds.join("','")}')`
      );

      const emailMessageUpdates = [];

      emailMessages.records.forEach(emailMsg => {
        const eventData = eventsByMessageId[emailMsg.MessageIdentifier];
        if (eventData) {
          const update = { Id: emailMsg.Id };
          
          // Calculate open count
          if (eventData.opens.length > 0) {
            const currentOpenCount = emailMsg.Open_Count__c || 0;
            update.Open_Count__c = currentOpenCount + eventData.opens.length;
            
            // Set first opened date if not already set
            if (!emailMsg.First_Opened_Date__c) {
              const firstOpen = eventData.opens.sort((a, b) => a.timestamp - b.timestamp)[0];
              update.First_Opened_Date__c = firstOpen.timestamp.toISOString();
            }
          }
          
          // Calculate click count
          if (eventData.clicks.length > 0) {
            const currentClickCount = emailMsg.Click_Count__c || 0;
            update.Click_Count__c = currentClickCount + eventData.clicks.length;
            
            // Update last clicked date
            const lastClick = eventData.clicks.sort((a, b) => b.timestamp - a.timestamp)[0];
            update.Last_Clicked_Date__c = lastClick.timestamp.toISOString();
            
            // Collect unique URLs clicked
            const urls = eventData.clicks.map(c => c.url).filter(u => u);
            const uniqueUrls = [...new Set(urls)];
            const existingUrls = emailMsg.Links_Clicked__c ? emailMsg.Links_Clicked__c.split('; ') : [];
            const allUrls = [...new Set([...existingUrls, ...uniqueUrls])];
            update.Links_Clicked__c = allUrls.join('; ').substring(0, 255); // Limit to field length
          }
          
          // Track bounces on EmailMessage
          if (eventData.bounce) {
            update.Bounce_Date__c = eventData.bounce.timestamp.toISOString();
            update.Bounce_Reason__c = eventData.bounce.reason;
          }
          
          emailMessageUpdates.push(update);
        }
      });

      if (emailMessageUpdates.length > 0) {
        await conn.sobject('EmailMessage').update(emailMessageUpdates);
        console.log(`Updated ${emailMessageUpdates.length} EmailMessage records`);
      }
    }

    // === PART 2: Update Lead aggregate metrics ===
    const emails = Object.keys(eventsByEmail);
    
    // Query Leads only (not converted)
    const leads = await conn.query(
      `SELECT Id, Email, Email_Open_Count__c, Email_Click_Count__c,
       Last_Email_Opened_Date__c, Last_Email_Clicked_Date__c
       FROM Lead WHERE Email IN ('${emails.join("','")}') AND IsConverted = false`
    );

    const leadUpdates = [];

    // Process Leads
    leads.records.forEach(lead => {
      const eventData = eventsByEmail[lead.Email];
      if (eventData) {
        const update = { Id: lead.Id };
        
        console.log(`Processing Lead: ${lead.Email} (ID: ${lead.Id})`);
        
        if (eventData.totalOpens > 0) {
          const currentCount = lead.Email_Open_Count__c || 0;
          const newCount = currentCount + eventData.totalOpens;
          update.Email_Open_Count__c = newCount;
          console.log(`  - Opens: ${currentCount} → ${newCount} (+${eventData.totalOpens})`);
          
          if (!lead.Last_Email_Opened_Date__c || 
              eventData.lastOpenDate > new Date(lead.Last_Email_Opened_Date__c)) {
            update.Last_Email_Opened_Date__c = eventData.lastOpenDate.toISOString();
            console.log(`  - Last Opened: ${eventData.lastOpenDate.toISOString()}`);
          }
        }
        
        if (eventData.totalClicks > 0) {
          const currentCount = lead.Email_Click_Count__c || 0;
          const newCount = currentCount + eventData.totalClicks;
          update.Email_Click_Count__c = newCount;
          console.log(`  - Clicks: ${currentCount} → ${newCount} (+${eventData.totalClicks})`);
          
          if (!lead.Last_Email_Clicked_Date__c || 
              eventData.lastClickDate > new Date(lead.Last_Email_Clicked_Date__c)) {
            update.Last_Email_Clicked_Date__c = eventData.lastClickDate.toISOString();
            console.log(`  - Last Clicked: ${eventData.lastClickDate.toISOString()}`);
          }
        }
        
        leadUpdates.push(update);
      }
    });

    // Check for emails that weren't found in Leads
    const emailsFoundInLeads = new Set(leads.records.map(l => l.Email));
    const emailsNotFound = emails.filter(email => !emailsFoundInLeads.has(email));
    
    if (emailsNotFound.length > 0) {
      console.log(`⚠️  ${emailsNotFound.length} email(s) not found in unconverted Leads:`);
      emailsNotFound.forEach(email => console.log(`  - ${email}`));
    }

    // Update Leads
    if (leadUpdates.length > 0) {
      await conn.sobject('Lead').update(leadUpdates);
      console.log(`✅ Successfully updated ${leadUpdates.length} Leads`);
    } else {
      console.log('ℹ️  No unconverted Leads found to update');
    }

    console.log('Batch processing complete');
    console.log('---');

  } catch (error) {
    console.error('❌ Batch processing error:', error);
    console.error('Error details:', error.message);
  }
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
