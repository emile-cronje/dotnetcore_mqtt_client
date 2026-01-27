namespace MqttTestClient.Services;

public class EntityContainer
{
    private readonly Lock _deleteMessageIdLock = new();
    private readonly HashSet<string> _deleteMessageIds = [];
    private readonly HashSet<string> _sentMessageIds = [];
    private readonly HashSet<string> _receivedMessageIds = [];    
    private readonly Lock _deleteSendCompleteLock = new();
    private readonly Lock _insertMessageIdLock = new();
    private readonly Lock _receivedMessageIdLock = new();    
    private readonly Lock _sentMessageIdLock = new();    
    private readonly HashSet<string> _insertMessageIds = [];
    private readonly Lock _insertSendCompleteLock = new();
    private readonly Lock _updateMessageIdLock = new();
    private readonly HashSet<string> _updateMessageIds = [];
    private readonly Lock _updateSendCompleteLock = new();
    private bool _hasDeleteSendCompleted;
    private bool _hasInsertSendCompleted;
    private bool _hasUpdateSendCompleted;
    
    public bool HasInsertSendCompleted
    {
        get
        {
            lock (_insertSendCompleteLock)
            {
                return _hasInsertSendCompleted;
            }
        }
        set
        {
            lock (_insertSendCompleteLock)
            {
                _hasInsertSendCompleted = value;
            }
        }
    }

    public bool HasUpdateSendCompleted
    {
        get
        {
            lock (_updateSendCompleteLock)
            {
                return _hasUpdateSendCompleted;
            }
        }
        set
        {
            lock (_updateSendCompleteLock)
            {
                _hasUpdateSendCompleted = value;
            }
        }
    }

    public bool HasDeleteSendCompleted
    {
        get
        {
            lock (_deleteSendCompleteLock)
            {
                return _hasDeleteSendCompleted;
            }
        }
        set
        {
            lock (_deleteSendCompleteLock)
            {
                _hasDeleteSendCompleted = value;
            }
        }
    }

    public void AddInsertMessageId(string messageId)
    {
        lock (_insertMessageIdLock)
        {
            if (!_insertMessageIds.Add(messageId))
            {
                throw new InvalidOperationException($"Duplicate insert message ID: {messageId}");
            }
            
            if (!_sentMessageIds.Add(messageId))
            {
                throw new InvalidOperationException($"Insert: Duplicate sent message ID: {messageId}");
            }            
        }
    }

    public void AddUpdateMessageId(string messageId)
    {
        lock (_updateMessageIdLock)
        {
            if (!_updateMessageIds.Add(messageId))
            {
                throw new InvalidOperationException($"Duplicate update message ID: {messageId}");
            }
            
            if (!_sentMessageIds.Add(messageId))
            {
                throw new InvalidOperationException($"Update: Duplicate sent message ID: {messageId}");
            }            
        }
    }

    public void AddDeleteMessageId(string messageId)
    {
        lock (_deleteMessageIdLock)
        {
            if (!_deleteMessageIds.Add(messageId))
            {
                throw new InvalidOperationException($"Duplicate delete message ID: {messageId}");
            }            
            
            if (!_sentMessageIds.Add(messageId))
            {
                throw new InvalidOperationException($"Delete: Duplicate sent message ID: {messageId}");
            }            
        }
    }

    public void RemoveUpdateMessageId(string messageId)
    {
        lock (_updateMessageIdLock)
        {
            if (!_updateMessageIds.Contains(messageId))
                Console.WriteLine("Id not found..." + messageId);
            else
                _updateMessageIds.Remove(messageId);
            
            if (!_receivedMessageIds.Add(messageId))
            {
                throw new InvalidOperationException($"Duplicate received message ID: {messageId}");
            }            
        }
    }

    public void RemoveUpdateMessageIds(List<string> messageIds)
    {
        lock (_updateMessageIdLock)
        {
            var notPresent = messageIds.Where(item => !_updateMessageIds.Contains(item)).ToList();
            
            if (notPresent.Count > 0)
                Console.WriteLine("Missing update messages: " + string.Join(", ", notPresent));
            
            _updateMessageIds.ExceptWith(messageIds);
        }
    }

    public void RemoveUpdateMessageIdOn404(string messageId)
    {
        lock (_updateMessageIdLock)
        {
            if (!_updateMessageIds.Contains(messageId))
                Console.WriteLine("Update message ID not found for 404: " + messageId);
            else
                _updateMessageIds.Remove(messageId);
        }
    }
    
    public void RemoveDeleteMessageId(string messageId)
    {
        lock (_deleteMessageIdLock)
        {
            _deleteMessageIds.Remove(messageId);
            
            if (!_receivedMessageIds.Add(messageId))
            {
                throw new InvalidOperationException($"Duplicate received message ID: {messageId}");
            }            
        }
    }

    public void RemoveDeleteMessageIds(List<string> messageIds)
    {
        lock (_deleteMessageIdLock)
        {
            _deleteMessageIds.ExceptWith(messageIds);
        }
    }

    public void RemoveDeleteMessageIdOn404(string messageId)
    {
        lock (_deleteMessageIdLock)
        {
            if (!_deleteMessageIds.Contains(messageId))
                Console.WriteLine("Delete message ID not found for 404: " + messageId);
            else
                _deleteMessageIds.Remove(messageId);
        }
    }

    public void RemoveInsertMessageIds(List<string> messageIds)
    {
        lock (_insertMessageIdLock)
        {
            var notPresent = messageIds.Where(item => !_insertMessageIds.Contains(item)).ToList();
            
            if (notPresent.Count > 0)
                Console.WriteLine("Missing insert messages: " + string.Join(", ", notPresent));
            
            _insertMessageIds.ExceptWith(messageIds);
        }
    }
    
    public void RemoveInsertMessageId(string messageId)
    {
        lock (_insertMessageIdLock)
        {
            _insertMessageIds.Remove(messageId);
            
            if (!_receivedMessageIds.Add(messageId))
            {
                throw new InvalidOperationException($"Duplicate received message ID: {messageId}");
            }            
        }
    }

    public int GetInsertMessageIdsCount()
    {
        lock (_insertMessageIdLock)
        {
            return _insertMessageIds.Count;
        }
    }

    public int GetUpdateMessageIdsCount()
    {
        lock (_updateMessageIdLock)
        {
            return _updateMessageIds.Count;
        }
    }

    public int GetDeleteMessageIdsCount()
    {
        lock (_deleteMessageIdLock)
        {
            return _deleteMessageIds.Count;
        }
    }

    public HashSet<string> GetDeleteMessageIds()
    {
        lock (_deleteMessageIdLock)
        {
            return _deleteMessageIds;
        }
    }
    
    public HashSet<string> GetInsertMessageIds()
    {
        lock (_insertMessageIdLock)
        {
            return _insertMessageIds;
        }
    }    

    public HashSet<string> GetSentMessageIds()
    {
        lock (_sentMessageIdLock)
        {
            return _sentMessageIds;
        }
    }    

    public HashSet<string> GetReceivedMessageIds()
    {
        lock (_receivedMessageIdLock)
        {
            return _receivedMessageIds;
        }
    }    
    
    public HashSet<string> GetUpdateMessageIds()
    {
        lock (_updateMessageIdLock)
        {
            return _updateMessageIds;
        }
    }    
}