use serde::Serialize;

#[derive(Serialize, Debug)]
pub struct SendMessageBody {
    pub chat_id: String,
    pub text: String,
    pub disable_web_page_preview: bool,
    pub disable_notification: bool,
}
