import { useState } from 'react';
import axios from 'axios';
import '../styles/ContactForm.css';

export default function ContactForm() {
  const [formData, setFormData] = useState({
    full_name: '',
    email: '',
    mobile: '',
    message: ''
  });

  const [status, setStatus] = useState(null); // 'success', 'error', or null
  const [message, setMessage] = useState('');
  const [loading, setLoading] = useState(false);

  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData((prev) => ({
      ...prev,
      [name]: value
    }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setStatus(null);

    // Basic validation
    if (!formData.full_name.trim()) {
      setStatus('error');
      setMessage('Full Name is required');
      setLoading(false);
      return;
    }

    if (!formData.email.trim()) {
      setStatus('error');
      setMessage('Email is required');
      setLoading(false);
      return;
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(formData.email)) {
      setStatus('error');
      setMessage('Please enter a valid email');
      setLoading(false);
      return;
    }

    if (!formData.message.trim()) {
      setStatus('error');
      setMessage('Message is required');
      setLoading(false);
      return;
    }

    try {
      const response = await axios.post('http://localhost:8000/submit-contact', {
        full_name: formData.full_name,
        email: formData.email,
        mobile: formData.mobile || null,
        message: formData.message
      });

      setStatus('success');
      setMessage('Thank you! Your message has been sent successfully.');
      setFormData({
        full_name: '',
        email: '',
        mobile: '',
        message: ''
      });

      // Clear success message after 5 seconds
      setTimeout(() => {
        setStatus(null);
        setMessage('');
      }, 5000);

    } catch (error) {
      setStatus('error');
      if (error.response?.data?.detail) {
        setMessage(`Error: ${error.response.data.detail}`);
      } else {
        setMessage('Error submitting form. Please try again.');
      }
      console.error('Error:', error);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="contact-container">
      <h1 className="contact-title">Contact Us</h1>
      <div className="contact-underline"></div>

      {status === 'success' && (
        <div className="alert alert-success">{message}</div>
      )}

      {status === 'error' && (
        <div className="alert alert-error">{message}</div>
      )}

      <form onSubmit={handleSubmit} className="contact-form">
        <div className="form-group">
          <label htmlFor="full_name" className="form-label">
            Full Name <span className="required">*</span>
          </label>
          <input
            type="text"
            id="full_name"
            name="full_name"
            value={formData.full_name}
            onChange={handleChange}
            className="form-input"
            placeholder=""
            disabled={loading}
          />
        </div>

        <div className="form-group">
          <label htmlFor="email" className="form-label">
            Email <span className="required">*</span>
          </label>
          <input
            type="email"
            id="email"
            name="email"
            value={formData.email}
            onChange={handleChange}
            className="form-input"
            placeholder=""
            disabled={loading}
          />
        </div>

        <div className="form-group">
          <label htmlFor="mobile" className="form-label">
            Mobile
          </label>
          <input
            type="tel"
            id="mobile"
            name="mobile"
            value={formData.mobile}
            onChange={handleChange}
            className="form-input"
            placeholder=""
            disabled={loading}
          />
        </div>

        <div className="form-group">
          <label htmlFor="message" className="form-label">
            Message <span className="required">*</span>
          </label>
          <textarea
            id="message"
            name="message"
            value={formData.message}
            onChange={handleChange}
            className="form-textarea"
            placeholder=""
            rows="6"
            disabled={loading}
          ></textarea>
        </div>

        <button
          type="submit"
          className="submit-button"
          disabled={loading}
        >
          {loading ? 'Submitting...' : 'Submit'}
        </button>
      </form>
    </div>
  );
}