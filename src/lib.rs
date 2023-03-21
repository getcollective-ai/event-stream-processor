//! <https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events#event_stream_format>

use futures::{AsyncBufRead, AsyncBufReadExt, StreamExt};
use tokio_stream::wrappers::ReceiverStream;

#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct Message {
    /// A string identifying the type of event described. If this is specified, an event will be
    /// dispatched on the browser to the listener for the specified event name; the website source
    /// code should use addEventListener() to listen for named events. The onmessage handler is
    /// called if no event name is specified for a message.
    pub event: Option<String>,
    pub data: Option<String>,

    /// the event ID
    pub id: Option<String>,

    /// the number of milliseconds to wait before attempting to send the event
    pub retry: Option<u64>,
}

pub fn get_messages(
    bytes: impl AsyncBufRead + Send + 'static + Unpin,
) -> ReceiverStream<anyhow::Result<Message>> {
    let (tx, rx) = tokio::sync::mpsc::channel(100);
    tokio::spawn(async move {
        let mut message = Message::default();
        let mut lines = bytes.lines();
        while let Some(Ok(s)) = lines.next().await {
            if s.is_empty() {
                let message = std::mem::take(&mut message);
                if tx.send(Ok(message)).await.is_err() {
                    return;
                }
                continue;
            }
            let Some((k, v)) = s.split_once(':') else {
                let _ = tx.send(Err(anyhow::anyhow!("Invalid line: {}", s))).await;
                return;
            };
            let v = v.trim();
            match k.trim() {
                "event" => message.event = Some(v.to_string()),
                "data" => message.data = Some(v.to_string()),
                "id" => message.id = Some(v.to_string()),
                "retry" => match v.parse() {
                    Ok(v) => message.retry = Some(v),
                    Err(e) => {
                        let _ = tx
                            .send(Err(anyhow::anyhow!("Invalid retry value: {}", e)))
                            .await;
                        return;
                    }
                },
                _ => {
                    let _ = tx.send(Err(anyhow::anyhow!("Invalid line: {}", s))).await;
                    return;
                }
            }
        }

        if message != Message::default() {
            let _ = tx.send(Ok(message)).await;
        }
    });

    rx.into()
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use bytes::Bytes;
    use futures::TryStreamExt;
    use tokio::sync::mpsc::channel;
    use tokio_stream::{wrappers::ReceiverStream, StreamExt};

    use super::*;

    #[tokio::test]
    async fn test_correct_messages() {
        let (tx, rx) = channel(100);
        let stream = ReceiverStream::new(rx);
        let stream = stream.map(Ok).into_async_read();
        let mut messages = get_messages(stream);
        tokio::spawn(async move {
            tx.send(Bytes::from("event: foo\n")).await.unwrap();
            tx.send(Bytes::from("data: bar\n")).await.unwrap();
            tx.send(Bytes::from("\n")).await.unwrap();
            tx.send(Bytes::from("event: foo\n")).await.unwrap();
            tx.send(Bytes::from("data: bar\n")).await.unwrap();
            tx.send(Bytes::from("id: 1")).await.unwrap();
        });
        let message = messages.next().await.unwrap().unwrap();
        assert_eq!(message.event, Some("foo".to_string()));
        assert_eq!(message.data, Some("bar".to_string()));

        let message = messages.next().await.unwrap().unwrap();
        assert_eq!(message.event, Some("foo".to_string()));
        assert_eq!(message.data, Some("bar".to_string()));
        assert_eq!(message.id, Some("1".to_string()));
    }

    #[tokio::test]
    async fn test_incorrect_messages() {
        let (tx, rx) = channel(100);
        let stream = ReceiverStream::new(rx);
        let stream = stream.map(Ok).into_async_read();
        let mut messages = get_messages(stream);

        tokio::spawn(async move {
            tx.send(Bytes::from("event: foo\n")).await.unwrap();
            tx.send(Bytes::from("data: bar\n")).await.unwrap();
            tx.send(Bytes::from("id: 1\n")).await.unwrap();
            tx.send(Bytes::from("retry: 1\n")).await.unwrap();
            tx.send(Bytes::from("foo: bar\n")).await.unwrap();
        });

        assert_matches!(messages.next().await.unwrap(), Err(..));
    }
}
