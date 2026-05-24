package payment

import (
	"context"
	"testing"

	analyticsv1 "github.com/martketplace-vkr/analytics/pkg/api/grpc/v1"
	analyticspb "github.com/martketplace-vkr/analytics/pkg/api/grpc/v1/admin"
	processorconfig "github.com/martketplace-vkr/payment/internal/app/cmp/processor"
	"github.com/martketplace-vkr/pkg/utils/currency"
	"google.golang.org/grpc"
)

type analyticsAdminClientStub struct {
	commissionPercent string
}

func (s analyticsAdminClientStub) ListTariffs(context.Context, *analyticspb.ListTariffsRequest, ...grpc.CallOption) (*analyticspb.ListTariffsResponse, error) {
	return nil, nil
}

func (s analyticsAdminClientStub) CreateTariff(context.Context, *analyticspb.CreateTariffRequest, ...grpc.CallOption) (*analyticspb.TariffResponse, error) {
	return nil, nil
}

func (s analyticsAdminClientStub) UpdateTariff(context.Context, *analyticspb.UpdateTariffRequest, ...grpc.CallOption) (*analyticspb.TariffResponse, error) {
	return nil, nil
}

func (s analyticsAdminClientStub) SetDefaultTariff(context.Context, *analyticspb.SetDefaultTariffRequest, ...grpc.CallOption) (*analyticspb.TariffResponse, error) {
	return nil, nil
}

func (s analyticsAdminClientStub) AssignVendorTariff(context.Context, *analyticspb.AssignVendorTariffRequest, ...grpc.CallOption) (*analyticspb.VendorTariffResponse, error) {
	return nil, nil
}

func (s analyticsAdminClientStub) GetVendorTariff(context.Context, *analyticspb.GetVendorTariffRequest, ...grpc.CallOption) (*analyticspb.VendorTariffResponse, error) {
	return &analyticspb.VendorTariffResponse{
		VendorId: 1,
		Tariff: &analyticspb.Tariff{
			Id:                1,
			Name:              "test",
			CommissionPercent: s.commissionPercent,
		},
	}, nil
}

func TestCalculateSettlement(t *testing.T) {
	tests := []struct {
		name              string
		amount            string
		currencyCode      int64
		commissionPercent string
		wantVendor        string
		wantFee           string
	}{
		{
			name:              "rub default commission",
			amount:            "100.00",
			currencyCode:      int64(currency.RUB),
			commissionPercent: "5.00",
			wantVendor:        "95.00",
			wantFee:           "5.00",
		},
		{
			name:              "rub half up rounding",
			amount:            "10.05",
			currencyCode:      int64(currency.RUB),
			commissionPercent: "5.00",
			wantVendor:        "9.55",
			wantFee:           "0.50",
		},
		{
			name:              "zero commission",
			amount:            "100.00",
			currencyCode:      int64(currency.RUB),
			commissionPercent: "0.00",
			wantVendor:        "100.00",
			wantFee:           "0.00",
		},
		{
			name:              "full commission",
			amount:            "100.00",
			currencyCode:      int64(currency.RUB),
			commissionPercent: "100.00",
			wantVendor:        "0.00",
			wantFee:           "100.00",
		},
		{
			name:              "usdt precision",
			amount:            "1.00000000",
			currencyCode:      int64(currency.USDTinTRC),
			commissionPercent: "12.345678",
			wantVendor:        "0.87654322",
			wantFee:           "0.12345678",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := New(nil, nil, &analyticsv1.Connector{
				Admin: analyticsAdminClientStub{commissionPercent: tt.commissionPercent},
			}, nil, processorconfig.Config{})

			vendorAmount, fee, err := svc.calculateSettlement(context.Background(), tt.amount, tt.currencyCode, 1)
			if err != nil {
				t.Fatalf("calculateSettlement returned error: %v", err)
			}
			if vendorAmount != tt.wantVendor {
				t.Fatalf("vendor amount = %s, want %s", vendorAmount, tt.wantVendor)
			}
			if fee != tt.wantFee {
				t.Fatalf("fee = %s, want %s", fee, tt.wantFee)
			}
		})
	}
}
