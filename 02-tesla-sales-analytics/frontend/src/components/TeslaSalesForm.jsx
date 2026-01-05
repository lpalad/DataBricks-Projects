import { useState } from 'react';
import axios from 'axios';
import '../styles/TeslaSalesForm.css';

export default function TeslaSalesForm() {
  const teslaModels = ['Model 3', 'Model Y', 'Model S', 'Model X', 'Cybertruck'];
  const colors = ['Pearl White Multi-Coat', 'Solid Black', 'Stealth Grey', 'Ultra Violet', 'Midnight Blue', 'Solid Red'];
  const paymentMethods = ['Cash', 'Finance', 'Trade-in', 'Lease'];
  const deliveryStatuses = ['Ordered', 'In Production', 'In Transit', 'Delivered', 'Pending'];

  const [formData, setFormData] = useState({
    // Customer fields
    CustomerName: '',
    CustomerLastName: '',
    DOB: '',
    Suburb: '',
    State: '',
    // Sales fields
    ProductModel: '',
    Color: '',
    DatePurchase: new Date().toISOString().split('T')[0],
    SalesPersonID: '',
    PaymentMethod: '',
    DeliveryStatus: 'Ordered',
    ProductDescription: '',
    BasePrice: '',
    OptionsPrice: ''
  });

  const [status, setStatus] = useState(null);
  const [message, setMessage] = useState('');
  const [loading, setLoading] = useState(false);

  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData((prev) => ({
      ...prev,
      [name]: value
    }));
  };

  const validateForm = () => {
    if (!formData.CustomerName.trim()) {
      setStatus('error');
      setMessage('First Name is required');
      return false;
    }
    if (!formData.CustomerLastName.trim()) {
      setStatus('error');
      setMessage('Last Name is required');
      return false;
    }
    if (!formData.DOB) {
      setStatus('error');
      setMessage('Date of Birth is required');
      return false;
    }
    if (!formData.Suburb.trim()) {
      setStatus('error');
      setMessage('Suburb is required');
      return false;
    }
    if (!formData.State) {
      setStatus('error');
      setMessage('State is required');
      return false;
    }
    if (!formData.ProductModel) {
      setStatus('error');
      setMessage('Product Model is required');
      return false;
    }
    if (!formData.Color) {
      setStatus('error');
      setMessage('Color is required');
      return false;
    }
    if (!formData.BasePrice || parseFloat(formData.BasePrice) <= 0) {
      setStatus('error');
      setMessage('Base Price must be greater than 0');
      return false;
    }
    return true;
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setStatus(null);

    if (!validateForm()) {
      setLoading(false);
      return;
    }

    try {
      const basePrice = parseFloat(formData.BasePrice);
      const optionsPrice = parseFloat(formData.OptionsPrice) || 0;
      const purchaseAmount = basePrice + optionsPrice;
      const gstAmount = purchaseAmount * 0.1;
      const totalPurchase = purchaseAmount + gstAmount;

      const response = await axios.post('http://localhost:8001/submit-tesla-order', {
        // Customer fields
        CustomerName: formData.CustomerName,
        CustomerLastName: formData.CustomerLastName,
        DOB: formData.DOB,
        Suburb: formData.Suburb,
        State: formData.State,
        // Sales fields
        ProductModel: formData.ProductModel,
        Color: formData.Color,
        DatePurchase: formData.DatePurchase,
        SalesPersonID: formData.SalesPersonID || null,
        PaymentMethod: formData.PaymentMethod || null,
        DeliveryStatus: formData.DeliveryStatus,
        ProductDescription: formData.ProductDescription || null,
        BasePrice: basePrice,
        OptionsPrice: optionsPrice,
        PurchaseAmount: purchaseAmount,
        GSTAmount: gstAmount,
        TotalPurchase: totalPurchase
      });

      setStatus('success');
      setMessage('Tesla order submitted successfully!');
      
      // Reset form
      setFormData({
        CustomerName: '',
        CustomerLastName: '',
        DOB: '',
        Suburb: '',
        State: '',
        ProductModel: '',
        Color: '',
        DatePurchase: new Date().toISOString().split('T')[0],
        SalesPersonID: '',
        PaymentMethod: '',
        DeliveryStatus: 'Ordered',
        ProductDescription: '',
        BasePrice: '',
        OptionsPrice: ''
      });

      setTimeout(() => {
        setStatus(null);
        setMessage('');
      }, 5000);

    } catch (error) {
      setStatus('error');
      if (error.response?.data?.detail) {
        setMessage(`Error: ${error.response.data.detail}`);
      } else {
        setMessage('Error submitting order. Please try again.');
      }
      console.error('Error:', error);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="tesla-form-container">
      <h1 className="tesla-title">Tesla Sales Order</h1>
      <div className="tesla-underline"></div>

      {status === 'success' && (
        <div className="alert alert-success">{message}</div>
      )}

      {status === 'error' && (
        <div className="alert alert-error">{message}</div>
      )}

      <form onSubmit={handleSubmit} className="tesla-form">
        {/* Customer Section */}
        <fieldset className="form-section">
          <legend className="section-title">Customer Information</legend>

          <div className="form-row">
            <div className="form-group">
              <label htmlFor="CustomerName" className="form-label">
                First Name <span className="required">*</span>
              </label>
              <input
                type="text"
                id="CustomerName"
                name="CustomerName"
                value={formData.CustomerName}
                onChange={handleChange}
                className="form-input"
                disabled={loading}
              />
            </div>

            <div className="form-group">
              <label htmlFor="CustomerLastName" className="form-label">
                Last Name <span className="required">*</span>
              </label>
              <input
                type="text"
                id="CustomerLastName"
                name="CustomerLastName"
                value={formData.CustomerLastName}
                onChange={handleChange}
                className="form-input"
                disabled={loading}
              />
            </div>
          </div>

          <div className="form-row">
            <div className="form-group">
              <label htmlFor="DOB" className="form-label">
                Date of Birth <span className="required">*</span>
              </label>
              <input
                type="date"
                id="DOB"
                name="DOB"
                value={formData.DOB}
                onChange={handleChange}
                className="form-input"
                disabled={loading}
              />
            </div>

            <div className="form-group">
              <label htmlFor="Suburb" className="form-label">
                Suburb <span className="required">*</span>
              </label>
              <input
                type="text"
                id="Suburb"
                name="Suburb"
                value={formData.Suburb}
                onChange={handleChange}
                className="form-input"
                disabled={loading}
              />
            </div>

            <div className="form-group">
              <label htmlFor="State" className="form-label">
                State <span className="required">*</span>
              </label>
              <select
                id="State"
                name="State"
                value={formData.State}
                onChange={handleChange}
                className="form-input"
                disabled={loading}
              >
                <option value="">Select State</option>
                <option value="NSW">NSW</option>
                <option value="VIC">VIC</option>
                <option value="QLD">QLD</option>
                <option value="WA">WA</option>
                <option value="SA">SA</option>
                <option value="TAS">TAS</option>
                <option value="ACT">ACT</option>
                <option value="NT">NT</option>
              </select>
            </div>
          </div>
        </fieldset>

        {/* Product/Sales Section */}
        <fieldset className="form-section">
          <legend className="section-title">Product & Sales Information</legend>

          <div className="form-row">
            <div className="form-group">
              <label htmlFor="ProductModel" className="form-label">
                Tesla Model <span className="required">*</span>
              </label>
              <select
                id="ProductModel"
                name="ProductModel"
                value={formData.ProductModel}
                onChange={handleChange}
                className="form-input"
                disabled={loading}
              >
                <option value="">Select Model</option>
                {teslaModels.map(model => (
                  <option key={model} value={model}>{model}</option>
                ))}
              </select>
            </div>

            <div className="form-group">
              <label htmlFor="Color" className="form-label">
                Color <span className="required">*</span>
              </label>
              <select
                id="Color"
                name="Color"
                value={formData.Color}
                onChange={handleChange}
                className="form-input"
                disabled={loading}
              >
                <option value="">Select Color</option>
                {colors.map(color => (
                  <option key={color} value={color}>{color}</option>
                ))}
              </select>
            </div>
          </div>

          <div className="form-row">
            <div className="form-group">
              <label htmlFor="DatePurchase" className="form-label">
                Purchase Date
              </label>
              <input
                type="date"
                id="DatePurchase"
                name="DatePurchase"
                value={formData.DatePurchase}
                onChange={handleChange}
                className="form-input"
                disabled={loading}
              />
            </div>

            <div className="form-group">
              <label htmlFor="SalesPersonID" className="form-label">
                Sales Person ID
              </label>
              <input
                type="text"
                id="SalesPersonID"
                name="SalesPersonID"
                value={formData.SalesPersonID}
                onChange={handleChange}
                className="form-input"
                disabled={loading}
                placeholder="e.g., SP001"
              />
            </div>
          </div>

          <div className="form-row">
            <div className="form-group">
              <label htmlFor="PaymentMethod" className="form-label">
                Payment Method
              </label>
              <select
                id="PaymentMethod"
                name="PaymentMethod"
                value={formData.PaymentMethod}
                onChange={handleChange}
                className="form-input"
                disabled={loading}
              >
                <option value="">Select Payment Method</option>
                {paymentMethods.map(method => (
                  <option key={method} value={method}>{method}</option>
                ))}
              </select>
            </div>

            <div className="form-group">
              <label htmlFor="DeliveryStatus" className="form-label">
                Delivery Status
              </label>
              <select
                id="DeliveryStatus"
                name="DeliveryStatus"
                value={formData.DeliveryStatus}
                onChange={handleChange}
                className="form-input"
                disabled={loading}
              >
                {deliveryStatuses.map(status => (
                  <option key={status} value={status}>{status}</option>
                ))}
              </select>
            </div>
          </div>

          <div className="form-group">
            <label htmlFor="ProductDescription" className="form-label">
              Product Description / Notes
            </label>
            <textarea
              id="ProductDescription"
              name="ProductDescription"
              value={formData.ProductDescription}
              onChange={handleChange}
              className="form-textarea"
              rows="3"
              disabled={loading}
              placeholder="e.g., Long Range, All-Wheel Drive, Premium Interior..."
            ></textarea>
          </div>
        </fieldset>

        {/* Pricing Section */}
        <fieldset className="form-section">
          <legend className="section-title">Pricing</legend>

          <div className="form-row">
            <div className="form-group">
              <label htmlFor="BasePrice" className="form-label">
                Base Price (AUD) <span className="required">*</span>
              </label>
              <input
                type="number"
                id="BasePrice"
                name="BasePrice"
                value={formData.BasePrice}
                onChange={handleChange}
                className="form-input"
                disabled={loading}
                placeholder="0.00"
                step="0.01"
                min="0"
              />
            </div>

            <div className="form-group">
              <label htmlFor="OptionsPrice" className="form-label">
                Options Price (AUD)
              </label>
              <input
                type="number"
                id="OptionsPrice"
                name="OptionsPrice"
                value={formData.OptionsPrice}
                onChange={handleChange}
                className="form-input"
                disabled={loading}
                placeholder="0.00"
                step="0.01"
                min="0"
              />
            </div>
          </div>

          {formData.BasePrice && (
            <div className="pricing-summary">
              <div className="price-row">
                <span>Base Price:</span>
                <span>${parseFloat(formData.BasePrice).toFixed(2)}</span>
              </div>
              <div className="price-row">
                <span>Options Price:</span>
                <span>${parseFloat(formData.OptionsPrice || 0).toFixed(2)}</span>
              </div>
              <div className="price-row">
                <span>Subtotal:</span>
                <span>${(parseFloat(formData.BasePrice) + parseFloat(formData.OptionsPrice || 0)).toFixed(2)}</span>
              </div>
              <div className="price-row">
                <span>GST (10%):</span>
                <span>${((parseFloat(formData.BasePrice) + parseFloat(formData.OptionsPrice || 0)) * 0.1).toFixed(2)}</span>
              </div>
              <div className="price-row total">
                <span>Total Purchase:</span>
                <span>${((parseFloat(formData.BasePrice) + parseFloat(formData.OptionsPrice || 0)) * 1.1).toFixed(2)}</span>
              </div>
            </div>
          )}
        </fieldset>

        <button
          type="submit"
          className="submit-button"
          disabled={loading}
        >
          {loading ? 'Submitting Order...' : 'Submit Order'}
        </button>
      </form>
    </div>
  );
}