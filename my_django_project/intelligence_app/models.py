from django.db import models

class MarketIntelligenceReport(models.Model):
    # Meta data fields from the source Kafka event
    source_feed = models.CharField(max_length=255)
    original_alert_text = models.TextField()
    source_timestamp = models.DateTimeField()
    
    # Processing validation metadata fields
    validation_loops_count = models.IntegerField(default=1)
    status = models.CharField(
        max_length=50, 
        choices=[('Approved', 'Approved'), ('Rejected', 'Rejected')],
        default='Approved'
    )
    
    # Final AI-generated outputs
    generated_markdown_report = models.TextField()
    
    # Internal system tracking fields
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'market_intelligence_reports'
        ordering = ['-created_at']

    def __str__(self):
        return f"Report {self.id} - {self.source_feed} ({self.created_at.strftime('%Y-%m-%d')})"
